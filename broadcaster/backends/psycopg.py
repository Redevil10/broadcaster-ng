from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable
from typing import TypeVar
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql

from .._base import Event
from .._utils import stop_task, wait_any, wait_or_stop
from .base import BroadcastBackend

logger = logging.getLogger("broadcaster.psycopg")

T = TypeVar("T")
Connection = psycopg.AsyncConnection[tuple[object, ...]]


def _kill(conn: Connection) -> None:
    """Close the connection's socket at once, failing any pending query."""
    try:
        conn.pgconn.finish()
    except Exception:
        logger.debug("Error closing Postgres connection", exc_info=True)


async def _with_deadline(conn: Connection, awaitable: Awaitable[T], timeout: float) -> T:
    """Run a command on `conn`, closing the connection if it takes too long.

    Not asyncio.wait_for(): cancelling a psycopg query sends a cancel request
    to the server and waits up to 10 s for it, which on a half-open connection
    never arrives. Closing the socket ends the pending query at once; the
    connection is broken anyway.
    """
    task = asyncio.ensure_future(awaitable)
    task.add_done_callback(lambda t: t.cancelled() or t.exception())  # retrieve it
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        _kill(conn)
        await asyncio.wait({task}, timeout=1.0)
        raise
    if not done:
        _kill(conn)
        await asyncio.wait({task}, timeout=1.0)
        raise TimeoutError(f"no reply from Postgres within {timeout:g} s")
    return task.result()


def _describe(exc: BaseException) -> str:
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _libpq_url(url: str) -> str:
    """postgres+psycopg://... or postgresql+psycopg://... -> postgresql://..."""
    parts = urlsplit(url)
    return urlunsplit(parts._replace(scheme="postgresql"))


class PsycopgBackend(BroadcastBackend):
    """Postgres LISTEN/NOTIFY backend using psycopg 3 instead of asyncpg.

    Selected with a `postgres+psycopg://` or `postgresql+psycopg://` URL. Same
    behaviour as the asyncpg backend: one reader task owns the LISTEN
    connection, reconnects (re-running LISTEN for every channel that still has
    subscribers), and runs a heartbeat query when idle so a half-open connection
    is detected. libpq handles the `keepalives*` URL parameters itself.
    Publishing uses a separate connection. Notifications sent while
    disconnected are lost (NOTIFY is not stored).
    """

    _CONNECT_TIMEOUT = 5.0
    _COMMAND_TIMEOUT = 5.0  # deadline for LISTEN / UNLISTEN / heartbeat / NOTIFY
    _READ_TIMEOUT = 0.1  # one notifies() slice; commands wait at most this long for the reader
    _HEARTBEAT_AFTER = 10.0  # run the heartbeat after this long without a notification
    _SUBSCRIBE_TIMEOUT = 10.0  # how long subscribe() waits for an outage to end
    _RECONNECT_BACKOFF_INITIAL = 0.2
    _RECONNECT_BACKOFF_MAX = 5.0
    _LOG_INTERVAL = 60.0
    _STOP_TIMEOUT = 5.0
    _READER_RESTART_DELAY = 1.0
    _APPLICATION_NAME = "broadcaster-listener"

    def __init__(self, url: str):
        self._url = _libpq_url(url)
        # Channels that currently have subscribers: what to (re)LISTEN.
        self._channels: set[str] = set()
        # The live LISTEN connection, or None while disconnected. Invariant:
        # `_connected` is set exactly when `_conn` is not None, and then every
        # channel in `_channels` has been LISTENed on it.
        self._conn: Connection | None = None
        self._connected = asyncio.Event()
        self._lock = asyncio.Lock()  # serialises LISTEN/UNLISTEN/heartbeat and connection swaps
        self._publish_conn: Connection | None = None
        self._publish_lock = asyncio.Lock()
        self._listen_queue: asyncio.Queue[Event] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._last_seen = 0.0
        self._down_since: float | None = None

    async def connect(self) -> None:
        # Fail fast if Postgres is unreachable at startup.
        self._stop.clear()
        async with self._lock:
            await self._open()
        self._start_reader()

    async def disconnect(self) -> None:
        self._stop.set()
        reader, self._reader = self._reader, None
        if reader is not None and not await stop_task(reader, self._STOP_TIMEOUT):
            logger.warning("Postgres reader did not stop within %g s", self._STOP_TIMEOUT)
        conn, self._conn = self._conn, None
        self._connected.clear()
        if conn is not None:
            await self._close(conn)
        publish_conn, self._publish_conn = self._publish_conn, None
        if publish_conn is not None:
            await self._close(publish_conn)

    async def subscribe(self, channel: str) -> None:
        self._channels.add(channel)
        async with self._lock:
            conn = self._conn
            if conn is not None:
                try:
                    await _with_deadline(conn, self._execute(conn, "LISTEN", channel), self._COMMAND_TIMEOUT)
                    return
                except Exception as exc:
                    logger.warning(
                        "Postgres connection lost while subscribing to %r (%s); reconnecting", channel, _describe(exc)
                    )
                    await self._drop(conn)
        # No live connection: the reader LISTENs every channel in `_channels`
        # (including this one) when it reconnects. Wait for that, within a bound,
        # and stop waiting when the backend is being disconnected.
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
                await _with_deadline(conn, self._execute(conn, "UNLISTEN", channel), self._COMMAND_TIMEOUT)
            except Exception as exc:
                logger.warning(
                    "Postgres connection lost while unsubscribing from %r (%s); reconnecting", channel, _describe(exc)
                )
                await self._drop(conn)

    async def publish(self, channel: str, message: str) -> None:
        # A separate connection, so publishing does not queue behind (or get
        # stuck with) the LISTEN connection. Reopened after a failure.
        async with self._publish_lock:
            conn = self._publish_conn
            if conn is None or conn.closed:
                conn = self._publish_conn = await self._new_connection(application_name="broadcaster-publisher")
            try:
                await _with_deadline(
                    conn, conn.execute("SELECT pg_notify(%s, %s)", (channel, message)), self._COMMAND_TIMEOUT
                )
            except BaseException:
                self._publish_conn = None
                await self._close(conn)
                raise

    async def next_published(self) -> Event:
        return await self._listen_queue.get()

    @staticmethod
    async def _execute(conn: Connection, command: str, channel: str) -> None:
        await conn.execute(sql.SQL(command + " {}").format(sql.Identifier(channel)))

    async def _new_connection(self, application_name: str) -> Connection:
        return await asyncio.wait_for(
            psycopg.AsyncConnection.connect(
                self._url,
                autocommit=True,  # a LISTEN connection must not sit in a transaction
                connect_timeout=max(1, round(self._CONNECT_TIMEOUT)),
                application_name=application_name,
            ),
            self._CONNECT_TIMEOUT,
        )

    async def _open(self) -> None:
        """Open a new LISTEN connection and LISTEN every wanted channel.

        Must be called with `_lock` held.
        """
        conn = await self._new_connection(application_name=self._APPLICATION_NAME)
        try:
            for channel in list(self._channels):
                await _with_deadline(conn, self._execute(conn, "LISTEN", channel), self._COMMAND_TIMEOUT)
        except BaseException:
            await self._close(conn)
            raise
        self._conn = conn
        self._last_seen = time.monotonic()
        self._connected.set()

    def _is_current(self, conn: Connection) -> bool:
        return self._conn is conn

    async def _drop(self, conn: Connection) -> None:
        """Forget a broken connection; the reader will open a new one."""
        if self._is_current(conn):
            self._conn = None
            self._connected.clear()
            if self._down_since is None:
                self._down_since = time.monotonic()
        await self._close(conn)

    async def _close(self, conn: Connection) -> None:
        # Closing a half-open connection must not block: bound it.
        try:
            await asyncio.wait_for(conn.close(), self._COMMAND_TIMEOUT)
        except Exception:
            logger.debug("Error closing Postgres connection", exc_info=True)

    def _start_reader(self) -> None:
        if self._stop.is_set():
            return
        self._reader = asyncio.create_task(self._run())
        self._reader.add_done_callback(self._on_reader_done)

    def _on_reader_done(self, task: asyncio.Task[None]) -> None:
        # The reader handles connection errors itself and only returns when
        # stopped. If it ever ends otherwise (a bug), say so and restart it.
        if task.cancelled() or self._stop.is_set() or task is not self._reader:
            return
        logger.error(
            "Postgres reader stopped unexpectedly; restarting in %g s",
            self._READER_RESTART_DELAY,
            exc_info=task.exception(),
        )
        asyncio.get_running_loop().call_later(self._READER_RESTART_DELAY, self._start_reader)

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
                    downtime = time.monotonic() - self._down_since
                    logger.warning(
                        "Reconnected to Postgres after %.1f s; notifications sent meanwhile were lost", downtime
                    )
                    self._down_since = None
                    self._notify_reconnect(sorted(self._channels), downtime)
                failures = 0
                continue

            try:
                async for notify in conn.notifies(timeout=self._READ_TIMEOUT):
                    self._last_seen = time.monotonic()
                    self._listen_queue.put_nowait(Event(channel=notify.channel, message=notify.payload))
                if time.monotonic() - self._last_seen >= self._HEARTBEAT_AFTER:
                    await self._heartbeat(conn)
            except Exception as exc:
                if self._stop.is_set():
                    return
                logger.warning("Lost connection to Postgres (%s); reconnecting", _describe(exc))
                async with self._lock:
                    await self._drop(conn)

    async def _heartbeat(self, conn: Connection) -> None:
        async with self._lock:
            if not self._is_current(conn):
                return
            try:
                await _with_deadline(conn, conn.execute("SELECT 1"), self._COMMAND_TIMEOUT)
            except Exception as exc:
                if self._stop.is_set():
                    return
                logger.warning("Postgres heartbeat failed (%s); reconnecting", _describe(exc))
                await self._drop(conn)
                return
            self._last_seen = time.monotonic()
