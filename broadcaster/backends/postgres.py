from __future__ import annotations

import asyncio
import logging
import socket
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

from .._base import Event
from .._utils import stop_task, wait_any, wait_or_stop
from .base import BroadcastBackend

logger = logging.getLogger("broadcaster.postgres")

# libpq-style TCP keepalive URL parameters. asyncpg would forward them to the
# server as settings (and fail with "unrecognized configuration parameter"), so
# they are removed from the DSN and applied as socket options instead.
_KEEPALIVE_PARAMS = ("keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count")


def _describe(exc: BaseException) -> str:
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _split_keepalive_params(url: str) -> tuple[str, dict[str, int]]:
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    keepalive = {key: int(value) for key, value in query if key in _KEEPALIVE_PARAMS}
    rest = [(key, value) for key, value in query if key not in _KEEPALIVE_PARAMS]
    return urlunsplit(parts._replace(query=urlencode(rest))), keepalive


def _apply_keepalive(conn: asyncpg.Connection, options: dict[str, int]) -> None:
    # Like libpq: keepalive is on unless keepalives=0; idle/interval/count
    # override the OS defaults when given.
    sock = conn._transport.get_extra_info("socket")
    if sock is None or sock.family not in (socket.AF_INET, socket.AF_INET6):
        return  # e.g. a Unix socket
    if options.get("keepalives", 1) == 0:
        return
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)  # macOS
    for key, option in (
        ("keepalives_idle", idle_option),
        ("keepalives_interval", getattr(socket, "TCP_KEEPINTVL", None)),
        ("keepalives_count", getattr(socket, "TCP_KEEPCNT", None)),
    ):
        if key in options and option is not None:
            sock.setsockopt(socket.IPPROTO_TCP, option, options[key])


class PostgresBackend(BroadcastBackend):
    """Postgres LISTEN/NOTIFY backend that survives connection loss.

    One reader task owns the LISTEN connection: it reconnects (re-running LISTEN
    for every channel that still has subscribers) when the connection is
    terminated, and runs a heartbeat query when idle so that a half-open
    connection is detected. All commands on that connection are serialised and
    have deadlines. Publishing uses a separate small pool. Notifications sent
    while disconnected are lost (NOTIFY is not stored).
    """

    _CONNECT_TIMEOUT = 5.0
    _COMMAND_TIMEOUT = 5.0  # deadline for LISTEN / UNLISTEN / heartbeat / NOTIFY
    _HEARTBEAT_AFTER = 10.0  # run the heartbeat after this long without a notification
    _SUBSCRIBE_TIMEOUT = 10.0  # how long subscribe() waits for an outage to end
    _RECONNECT_BACKOFF_INITIAL = 0.2
    _RECONNECT_BACKOFF_MAX = 5.0
    _LOG_INTERVAL = 60.0
    _STOP_TIMEOUT = 5.0
    _PUBLISH_POOL_MAX_SIZE = 5
    _APPLICATION_NAME = "broadcaster-listener"

    def __init__(self, url: str):
        self._url, self._keepalive = _split_keepalive_params(url)
        # Channels that currently have subscribers: what to (re)LISTEN.
        self._channels: set[str] = set()
        # The live LISTEN connection, or None while disconnected. Invariant:
        # `_connected` is set exactly when `_conn` is not None, and then every
        # channel in `_channels` has been LISTENed on it.
        self._conn: asyncpg.Connection | None = None
        self._connected = asyncio.Event()
        # asyncpg allows one operation at a time per connection.
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()  # set by the termination listener
        self._listen_queue: asyncio.Queue[Event] = asyncio.Queue()
        self._pool: asyncpg.Pool | None = None
        self._stop = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._last_seen = 0.0
        self._down_since: float | None = None

    async def connect(self) -> None:
        # Fail fast if Postgres is unreachable at startup.
        self._stop.clear()
        self._pool = await asyncpg.create_pool(
            self._url,
            # min_size=0: no idle connection for asyncpg to restore in the
            # background during an outage (it logs a traceback for each try).
            min_size=0,
            max_size=self._PUBLISH_POOL_MAX_SIZE,
            timeout=self._CONNECT_TIMEOUT,
            command_timeout=self._COMMAND_TIMEOUT,
            init=self._init_publish_connection,
        )
        try:
            async with self._lock:
                await self._open()
        except BaseException:
            await self._pool.close()
            raise
        self._reader = asyncio.create_task(self._run())

    async def disconnect(self) -> None:
        self._stop.set()
        self._wake.set()
        reader, self._reader = self._reader, None
        if reader is not None and not await stop_task(reader, self._STOP_TIMEOUT):
            logger.warning("Postgres reader did not stop within %g s", self._STOP_TIMEOUT)
        conn, self._conn = self._conn, None
        self._connected.clear()
        if conn is not None:
            await self._close(conn)
        pool, self._pool = self._pool, None
        if pool is not None:
            try:
                await asyncio.wait_for(pool.close(), self._COMMAND_TIMEOUT)
            except Exception:
                pool.terminate()

    async def subscribe(self, channel: str) -> None:
        self._channels.add(channel)
        async with self._lock:
            conn = self._conn
            if conn is not None:
                try:
                    await asyncio.wait_for(conn.add_listener(channel, self._listener), self._COMMAND_TIMEOUT)
                    return
                except Exception as exc:
                    logger.warning(
                        "Postgres connection lost while subscribing to %r (%s); reconnecting", channel, _describe(exc)
                    )
                    self._drop(conn)
        # No live connection: the reader LISTENs every channel in `_channels`
        # (including this one) when it reconnects. Wait for that, within a bound.
        # Also stop waiting when the backend is being disconnected.
        await wait_any([self._connected, self._stop], self._SUBSCRIBE_TIMEOUT)
        if self._stop.is_set() or not self._connected.is_set():
            self._channels.discard(channel)
            if self._stop.is_set():
                raise ConnectionError(f"Postgres backend is disconnecting: could not subscribe to {channel!r}")
            raise ConnectionError(
                f"Postgres is unavailable: could not subscribe to {channel!r} within {self._SUBSCRIBE_TIMEOUT:g} s"
            )

    async def unsubscribe(self, channel: str) -> None:
        self._channels.discard(channel)
        async with self._lock:
            conn = self._conn
            if conn is None:
                return  # a reconnect will not LISTEN it again
            try:
                await asyncio.wait_for(conn.remove_listener(channel, self._listener), self._COMMAND_TIMEOUT)
            except Exception as exc:
                logger.warning(
                    "Postgres connection lost while unsubscribing from %r (%s); reconnecting", channel, _describe(exc)
                )
                self._drop(conn)

    async def publish(self, channel: str, message: str) -> None:
        # A separate pool, so publishing does not queue behind (or get stuck
        # with) the LISTEN connection.
        assert self._pool is not None, "connect() must be called first"
        async with self._pool.acquire(timeout=self._COMMAND_TIMEOUT) as conn:
            await conn.execute("SELECT pg_notify($1, $2);", channel, message, timeout=self._COMMAND_TIMEOUT)

    async def next_published(self) -> Event:
        return await self._listen_queue.get()

    def _listener(self, *args: Any) -> None:
        connection, pid, channel, payload = args
        self._last_seen = time.monotonic()
        self._listen_queue.put_nowait(Event(channel=channel, message=payload))

    def _on_termination(self, conn: asyncpg.Connection) -> None:
        self._wake.set()

    async def _init_publish_connection(self, conn: asyncpg.Connection) -> None:
        _apply_keepalive(conn, self._keepalive)

    async def _open(self) -> None:
        """Open a new LISTEN connection and LISTEN every wanted channel.

        Must be called with `_lock` held.
        """
        conn = await asyncpg.connect(
            self._url,
            timeout=self._CONNECT_TIMEOUT,
            command_timeout=self._COMMAND_TIMEOUT,
            server_settings={"application_name": self._APPLICATION_NAME},
        )
        try:
            _apply_keepalive(conn, self._keepalive)
            conn.add_termination_listener(self._on_termination)
            for channel in list(self._channels):
                await asyncio.wait_for(conn.add_listener(channel, self._listener), self._COMMAND_TIMEOUT)
        except BaseException:
            conn.terminate()
            raise
        self._wake.clear()
        self._conn = conn
        self._last_seen = time.monotonic()
        self._connected.set()

    def _is_current(self, conn: asyncpg.Connection) -> bool:
        return self._conn is conn

    def _drop(self, conn: asyncpg.Connection) -> None:
        """Forget a broken connection; the reader will open a new one."""
        if self._conn is conn:
            self._conn = None
            self._connected.clear()
            if self._down_since is None:
                self._down_since = time.monotonic()
        # terminate() closes the socket at once; a graceful close could block
        # on a half-open connection.
        conn.terminate()
        self._wake.set()

    async def _close(self, conn: asyncpg.Connection) -> None:
        try:
            await asyncio.wait_for(conn.close(), self._COMMAND_TIMEOUT)
        except Exception:
            conn.terminate()

    async def _run(self) -> None:
        failures = 0
        last_logged = 0.0
        while not self._stop.is_set():
            conn = self._conn
            if conn is None:
                try:
                    async with self._lock:
                        if self._conn is None:
                            await self._open()
                except Exception as exc:
                    if self._stop.is_set():
                        return
                    failures += 1
                    now = time.monotonic()
                    if failures == 1 or now - last_logged >= self._LOG_INTERVAL:
                        logger.error(
                            "Could not reconnect to Postgres (attempt %d): %s; retrying", failures, _describe(exc)
                        )
                        last_logged = now
                    delay = min(self._RECONNECT_BACKOFF_INITIAL * 2 ** (failures - 1), self._RECONNECT_BACKOFF_MAX)
                    await wait_or_stop(self._stop, delay)
                    continue
                if self._down_since is not None:
                    logger.warning(
                        "Reconnected to Postgres after %.1f s; notifications sent meanwhile were lost",
                        time.monotonic() - self._down_since,
                    )
                    self._down_since = None
                failures = 0
                continue

            # Sleep until woken (termination, drop, stop) or a heartbeat is due.
            idle = time.monotonic() - self._last_seen
            try:
                await asyncio.wait_for(self._wake.wait(), max(self._HEARTBEAT_AFTER - idle, 0.0))
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                return
            if not self._is_current(conn):
                continue  # dropped by subscribe/unsubscribe meanwhile
            if conn.is_closed():
                logger.warning("Lost connection to Postgres (connection closed); reconnecting")
                async with self._lock:
                    self._drop(conn)
                continue
            self._wake.clear()
            if time.monotonic() - self._last_seen >= self._HEARTBEAT_AFTER:
                await self._heartbeat(conn)

    async def _heartbeat(self, conn: asyncpg.Connection) -> None:
        async with self._lock:
            if not self._is_current(conn):
                return
            try:
                await asyncio.wait_for(conn.fetchval("SELECT 1"), self._COMMAND_TIMEOUT)
            except Exception as exc:
                if self._stop.is_set():
                    return
                logger.warning("Postgres heartbeat failed (%s); reconnecting", _describe(exc))
                self._drop(conn)
                return
            self._last_seen = time.monotonic()
