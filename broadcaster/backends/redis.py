from __future__ import annotations

import asyncio
import logging
import time
import typing

from redis import asyncio as redis, exceptions as redis_exceptions
from redis.asyncio.client import PubSub

from .._base import Event
from .._utils import stop_task, wait_any, wait_or_stop
from .base import BroadcastBackend

logger = logging.getLogger("broadcaster.redis")

# Errors that mean "this connection is unusable": drop it and reconnect.
_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    redis_exceptions.ConnectionError,
    redis_exceptions.TimeoutError,
    OSError,
    asyncio.TimeoutError,
    TimeoutError,
)


def _describe(exc: BaseException) -> str:
    # Not repr(): redis-py 8 exceptions have a repr without the message.
    text = str(exc)
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


def _to_str(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


class RedisBackend(BroadcastBackend):
    """Redis pub/sub backend that survives connection loss.

    One reader task owns the pub/sub connection: it reads messages, sends PING
    when idle, treats a connection that stays silent as dead, and reconnects
    with backoff, re-subscribing every channel that still has subscribers.
    Messages published while disconnected are lost (pub/sub is at-most-once).
    """

    # Defaults for clients created from a URL. URL query parameters override
    # them, e.g. redis://host:6379?socket_timeout=10&socket_connect_timeout=2.
    # Without socket_timeout, a reconnect to a server that accepts but never
    # answers blocks forever in the connection handshake.
    _DEFAULT_SOCKET_CONNECT_TIMEOUT = 5.0
    _DEFAULT_SOCKET_TIMEOUT = 5.0

    _READ_TIMEOUT = 1.0  # one get_message() poll
    _PING_AFTER = 10.0  # send PING after this long without receiving anything
    _SILENT_LIMIT = 30.0  # nothing received (not even a PONG) for this long: dead
    _COMMAND_TIMEOUT = 5.0  # deadline for connect / SUBSCRIBE / UNSUBSCRIBE / PING
    _SUBSCRIBE_TIMEOUT = 10.0  # how long subscribe() waits for an outage to end
    _RECONNECT_BACKOFF_INITIAL = 0.2
    _RECONNECT_BACKOFF_MAX = 5.0
    _LOG_INTERVAL = 60.0  # repeated reconnect failures: log at most this often
    _STOP_TIMEOUT = 5.0
    # A connection lost sooner than this after opening counts as a failed
    # attempt, so a server that accepts and then drops connections gets backoff.
    _STABLE_AFTER = 10.0

    _conn: redis.Redis

    def __init__(self, url: str | None = None, *, conn: redis.Redis | None = None):
        if url is None:
            assert conn is not None, "conn must be provided if url is not"
            self._conn = conn
        else:
            self._conn = redis.Redis.from_url(
                url,
                socket_connect_timeout=self._DEFAULT_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=self._DEFAULT_SOCKET_TIMEOUT,
            )

        # Channels that currently have subscribers: what to (re)subscribe.
        self._channels: set[str] = set()
        self._has_channels = asyncio.Event()
        # The live pub/sub connection, or None while disconnected. Invariant:
        # `_connected` is set exactly when `_pubsub` is not None, and then every
        # channel in `_channels` has been subscribed on it.
        self._pubsub: PubSub | None = None
        self._connected = asyncio.Event()
        # Serialises SUBSCRIBE/UNSUBSCRIBE and connection swaps.
        self._lock = asyncio.Lock()
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._last_seen = 0.0
        self._last_ping = 0.0
        # SUBSCRIBE, UNSUBSCRIBE and PING do not wait for the reply; the reader expects one
        # (or any other data) by this time, else the connection is dead.
        self._reply_due: float | None = None
        # subscribe() calls waiting for Redis to confirm SUBSCRIBE: set by the
        # reader when the confirmation arrives, or when it drops the connection.
        self._confirmations: dict[str, asyncio.Event] = {}
        self._down_since: float | None = None

    async def connect(self) -> None:
        # Fail fast if Redis is unreachable at startup (as upstream did);
        # later connection loss is handled by reconnecting.
        self._stop.clear()
        async with self._lock:
            await self._open()
        self._start_reader()

    async def disconnect(self) -> None:
        self._stop.set()
        reader, self._reader = self._reader, None
        if reader is not None and not await stop_task(reader, self._STOP_TIMEOUT):
            logger.warning("Redis reader did not stop within %g s", self._STOP_TIMEOUT)
        pubsub, self._pubsub = self._pubsub, None
        self._connected.clear()
        if pubsub is not None:
            await self._close(pubsub)
        await self._conn.aclose()

    async def subscribe(self, channel: str) -> None:
        self._channels.add(channel)
        self._has_channels.set()
        try:
            await self._subscribe(channel)
        except BaseException:
            # Failed or cancelled: Broadcast will not call unsubscribe() for
            # it, so it must not stay in `_channels` (re-subscribed forever).
            self._forget(channel)
            raise

    async def _subscribe(self, channel: str) -> None:
        confirmed = asyncio.Event()
        async with self._lock:
            pubsub = self._pubsub
            if pubsub is not None:
                self._confirmations[channel] = confirmed
                try:
                    # Armed before sending: the reader may see the reply before
                    # this call returns.
                    self._expect_reply()
                    await asyncio.wait_for(pubsub.subscribe(channel), self._COMMAND_TIMEOUT)
                except Exception as exc:
                    # Any failure means this connection is unusable; e.g. right
                    # after a reset, before the reader noticed, redis-py raises
                    # AttributeError on its closed writer, not ConnectionError.
                    logger.warning(
                        "Redis connection lost while subscribing to %r (%s); reconnecting", channel, _describe(exc)
                    )
                    await self._drop(pubsub)
                    pubsub = None
        if pubsub is not None:
            # SUBSCRIBE does not wait for its reply. Wait for the reader to see
            # it, so messages published after subscribe() returns are delivered.
            try:
                await wait_any([confirmed], self._COMMAND_TIMEOUT)
            finally:
                if self._confirmations.get(channel) is confirmed:
                    del self._confirmations[channel]
            if self._pubsub is pubsub:
                if confirmed.is_set():
                    return
                logger.warning("No reply from Redis to SUBSCRIBE %r; reconnecting", channel)
                async with self._lock:
                    await self._drop(pubsub)
        # No live connection: the reader re-subscribes every channel in
        # `_channels` (including this one) when it reconnects. Wait for that,
        # within a bound, so callers do not hang through a long outage.
        # Also stop waiting when the backend is being disconnected.
        await wait_any([self._connected, self._stop], self._SUBSCRIBE_TIMEOUT)
        if self._stop.is_set() or not self._connected.is_set():
            if self._stop.is_set():
                raise ConnectionError(f"Redis backend is disconnecting: could not subscribe to {channel!r}")
            raise ConnectionError(
                f"Redis is unavailable: could not subscribe to {channel!r} within {self._SUBSCRIBE_TIMEOUT:g} s"
            )

    async def unsubscribe(self, channel: str) -> None:
        self._forget(channel)
        async with self._lock:
            pubsub = self._pubsub
            if pubsub is None:
                return  # nothing to do: a reconnect will not re-subscribe it
            try:
                self._expect_reply()  # before sending: see subscribe()
                await asyncio.wait_for(pubsub.unsubscribe(channel), self._COMMAND_TIMEOUT)
            except Exception as exc:  # see subscribe()
                logger.warning(
                    "Redis connection lost while unsubscribing from %r (%s); reconnecting", channel, _describe(exc)
                )
                await self._drop(pubsub)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._conn.publish(channel, message)

    async def next_published(self) -> Event:
        return await self._queue.get()

    def _expect_reply(self) -> None:
        due = time.monotonic() + self._COMMAND_TIMEOUT
        if self._reply_due is None or due < self._reply_due:
            self._reply_due = due

    def _forget(self, channel: str) -> None:
        self._channels.discard(channel)
        if not self._channels:
            self._has_channels.clear()

    async def _open(self) -> None:
        """Open a new pub/sub connection and subscribe every wanted channel.

        Must be called with `_lock` held.
        """
        pubsub = self._conn.pubsub()
        try:
            await asyncio.wait_for(pubsub.connect(), self._COMMAND_TIMEOUT)  # type: ignore[no-untyped-call]
            if self._channels:
                channels = set(self._channels)
                await asyncio.wait_for(pubsub.subscribe(*channels), self._COMMAND_TIMEOUT)
                # Only report the connection as ready (and call on_reconnect)
                # once Redis has confirmed every SUBSCRIBE.
                await self._await_confirmations(pubsub, channels)
        except BaseException:
            await self._close(pubsub)
            raise
        self._pubsub = pubsub
        self._last_seen = self._last_ping = time.monotonic()
        self._reply_due = None
        self._connected.set()

    async def _await_confirmations(self, pubsub: PubSub, channels: set[str]) -> None:
        """Read from a new connection until every channel's SUBSCRIBE is confirmed,
        queueing any message that arrives meanwhile."""
        deadline = time.monotonic() + self._COMMAND_TIMEOUT
        while channels:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise redis_exceptions.ConnectionError(
                    f"no reply from Redis to SUBSCRIBE within {self._COMMAND_TIMEOUT:g} s"
                )
            timeout = min(remaining, self._READ_TIMEOUT)
            message = await asyncio.wait_for(pubsub.get_message(timeout=timeout), timeout + self._COMMAND_TIMEOUT)
            if message is None:
                continue
            if message["type"] == "subscribe":
                channels.discard(_to_str(message["channel"]))
            elif message["type"] == "message":
                self._queue.put_nowait(Event(channel=_to_str(message["channel"]), message=_to_str(message["data"])))

    async def _drop(self, pubsub: PubSub) -> None:
        """Forget a broken connection; the reader will open a new one."""
        if self._pubsub is pubsub:
            self._pubsub = None
            self._connected.clear()
            # Wake subscribe() calls waiting for a confirmation on this connection.
            for confirmed in self._confirmations.values():
                confirmed.set()
            self._confirmations.clear()
            if self._down_since is None:
                self._down_since = time.monotonic()
        await self._close(pubsub)

    async def _close(self, pubsub: PubSub) -> None:
        # Closing a half-open connection can block too; bound it.
        try:
            await asyncio.wait_for(pubsub.aclose(), self._COMMAND_TIMEOUT)  # type: ignore[no-untyped-call]
        except Exception:
            logger.debug("Error closing Redis pub/sub connection", exc_info=True)

    _READER_RESTART_DELAY = 1.0

    def _start_reader(self) -> None:
        if self._stop.is_set():
            return
        self._reader = asyncio.create_task(self._run())
        self._reader.add_done_callback(self._on_reader_done)

    def _on_reader_done(self, task: asyncio.Task[None]) -> None:
        # The reader handles connection errors itself and only returns when
        # stopped. If it ever ends otherwise (a bug), say so and restart it, so
        # delivery never stops silently.
        if task.cancelled() or self._stop.is_set() or task is not self._reader:
            return
        logger.error(
            "Redis reader stopped unexpectedly; restarting in %g s",
            self._READER_RESTART_DELAY,
            exc_info=task.exception(),
        )
        asyncio.get_running_loop().call_later(self._READER_RESTART_DELAY, self._start_reader)

    def _backoff(self, failures: int) -> float:
        return min(self._RECONNECT_BACKOFF_INITIAL * 2.0 ** min(failures - 1, 16), self._RECONNECT_BACKOFF_MAX)

    async def _run(self) -> None:
        failures = 0  # for the backoff; includes connections lost right after opening
        attempts = 0  # failed reconnect attempts since the last successful one, for the log
        last_logged = 0.0
        connected_at: float | None = time.monotonic()  # the connection opened by connect()
        while not self._stop.is_set():
            pubsub = self._pubsub
            if pubsub is None:
                if connected_at is not None:
                    # The connection was just lost.
                    lived = time.monotonic() - connected_at
                    connected_at = None
                    if lived < self._STABLE_AFTER:
                        failures += 1
                        await wait_or_stop(self._stop, self._backoff(failures))
                        continue
                    failures = 0
                try:
                    async with self._lock:
                        if self._pubsub is None:
                            await self._open()
                except Exception as exc:
                    if self._stop.is_set():
                        return
                    failures += 1
                    attempts += 1
                    now = time.monotonic()
                    if attempts == 1 or now - last_logged >= self._LOG_INTERVAL:
                        logger.error(
                            "Could not reconnect to Redis (attempt %d): %s; retrying",
                            attempts,
                            _describe(exc),
                            exc_info=not isinstance(exc, _CONNECTION_ERRORS),
                        )
                        last_logged = now
                    await wait_or_stop(self._stop, self._backoff(failures))
                    continue
                if self._down_since is not None:
                    downtime = time.monotonic() - self._down_since
                    logger.warning(
                        "Reconnected to Redis after %.1f s; messages published meanwhile were lost",
                        downtime,
                    )
                    self._down_since = None
                    self._notify_reconnect(sorted(self._channels), downtime)
                attempts = 0
                connected_at = time.monotonic()
                continue

            if not self._channels:
                # redis-py cannot read a pub/sub connection without subscriptions.
                try:
                    await asyncio.wait_for(self._has_channels.wait(), self._READ_TIMEOUT)
                except asyncio.TimeoutError:
                    pass
                self._last_seen = self._last_ping = time.monotonic()
                continue

            try:
                await self._read_once(pubsub)
            except Exception as exc:
                if self._stop.is_set():
                    return
                if isinstance(exc, _CONNECTION_ERRORS):
                    logger.warning("Lost connection to Redis (%s); reconnecting", _describe(exc))
                else:
                    logger.error("Unexpected error reading from Redis; reconnecting", exc_info=True)
                async with self._lock:
                    await self._drop(pubsub)

    async def _read_once(self, pubsub: PubSub) -> None:
        message = await pubsub.get_message(timeout=self._READ_TIMEOUT)
        now = time.monotonic()
        if message is not None:
            # Anything counts as a sign of life, including subscribe
            # confirmations and the PONG to our own PING.
            self._last_seen = now
            self._reply_due = None
            if message["type"] == "message":
                await self._queue.put(Event(channel=_to_str(message["channel"]), message=_to_str(message["data"])))
            elif message["type"] == "subscribe":
                confirmed = self._confirmations.pop(_to_str(message["channel"]), None)
                if confirmed is not None:
                    confirmed.set()
            return
        if self._reply_due is not None and now >= self._reply_due:
            raise redis_exceptions.ConnectionError(f"no reply from Redis within {self._COMMAND_TIMEOUT:g} s")
        if now - self._last_seen >= self._SILENT_LIMIT:
            raise redis_exceptions.ConnectionError(f"no data from Redis for {self._SILENT_LIMIT:g} s")
        if now - self._last_seen >= self._PING_AFTER and now - self._last_ping >= self._PING_AFTER:
            self._last_ping = now
            await asyncio.wait_for(pubsub.ping(), self._COMMAND_TIMEOUT)
            self._expect_reply()


StreamMessageType = tuple[bytes, tuple[tuple[bytes, dict[bytes, bytes]]]]


class RedisStreamBackend(BroadcastBackend):
    """Redis Streams backend.

    A stream stores its messages and the backend remembers the last ID it read
    per stream, so after a connection error (which `Broadcast` logs and
    retries) reading continues where it stopped: nothing is lost.
    """

    # Same defaults as RedisBackend; URL query parameters override them.
    # Without socket_timeout, XREAD on a half-open connection waits forever.
    _DEFAULT_SOCKET_CONNECT_TIMEOUT = RedisBackend._DEFAULT_SOCKET_CONNECT_TIMEOUT
    _DEFAULT_SOCKET_TIMEOUT = RedisBackend._DEFAULT_SOCKET_TIMEOUT

    def __init__(self, url: str):
        url = url.replace("redis-stream", "redis", 1)
        self.streams: dict[bytes | str | memoryview, int | bytes | str | memoryview] = {}
        self._ready = asyncio.Event()
        self._producer = self._client(url)
        self._consumer = self._client(url)

    def _client(self, url: str) -> redis.Redis:
        # Annotated variable: redis-py 5 types from_url() as returning Any.
        client: redis.Redis = redis.Redis.from_url(
            url,
            socket_connect_timeout=self._DEFAULT_SOCKET_CONNECT_TIMEOUT,
            socket_timeout=self._DEFAULT_SOCKET_TIMEOUT,
        )
        return client

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        await self._producer.aclose()
        await self._consumer.aclose()

    async def subscribe(self, channel: str) -> None:
        try:
            info = await self._consumer.xinfo_stream(channel)
            last_id = info["last-generated-id"]
        except redis.ResponseError:
            last_id = "0"
        self.streams[channel] = last_id
        self._ready.set()

    async def unsubscribe(self, channel: str) -> None:
        self.streams.pop(channel, None)
        if not self.streams:
            # Nothing left to read: let wait_for_messages() wait again instead of
            # calling XREAD with no streams (which raises DataError).
            self._ready.clear()

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.xadd(channel, {"message": message})

    async def wait_for_messages(self) -> list[StreamMessageType]:
        messages = None
        while not messages:
            await self._ready.wait()
            if not self.streams:  # unsubscribed from the last stream meanwhile
                self._ready.clear()
                continue
            messages = await self._consumer.xread(self.streams, count=1, block=100)
        # redis-py >= 6 types xread() as a RESP2/RESP3 union; with RESP2 it is this list.
        return typing.cast(list[StreamMessageType], messages)

    async def next_published(self) -> Event:
        messages = await self.wait_for_messages()
        stream, events = messages[0]
        _msg_id, message = events[0]
        name = stream.decode("utf-8")
        # Only if still subscribed: writing it back unconditionally re-added a
        # stream that was unsubscribed while this message was in flight.
        if name in self.streams:
            self.streams[name] = _msg_id.decode("utf-8")
        return Event(
            channel=name,
            message=message.get(b"message", b"").decode("utf-8"),
        )
