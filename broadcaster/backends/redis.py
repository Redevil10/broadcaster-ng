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
        # SUBSCRIBE and PING do not wait for the reply; the reader expects one
        # (or any other data) by this time, else the connection is dead.
        self._reply_due: float | None = None
        self._down_since: float | None = None

    async def connect(self) -> None:
        # Fail fast if Redis is unreachable at startup (as upstream did);
        # later connection loss is handled by reconnecting.
        self._stop.clear()
        async with self._lock:
            await self._open()
        self._reader = asyncio.create_task(self._run())

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
        async with self._lock:
            pubsub = self._pubsub
            if pubsub is not None:
                try:
                    await asyncio.wait_for(pubsub.subscribe(channel), self._COMMAND_TIMEOUT)
                    self._expect_reply()
                    return
                except Exception as exc:
                    # Any failure means this connection is unusable; e.g. right
                    # after a reset, before the reader noticed, redis-py raises
                    # AttributeError on its closed writer, not ConnectionError.
                    logger.warning(
                        "Redis connection lost while subscribing to %r (%s); reconnecting", channel, _describe(exc)
                    )
                    await self._drop(pubsub)
        # No live connection: the reader re-subscribes every channel in
        # `_channels` (including this one) when it reconnects. Wait for that,
        # within a bound, so callers do not hang through a long outage.
        # Also stop waiting when the backend is being disconnected.
        await wait_any([self._connected, self._stop], self._SUBSCRIBE_TIMEOUT)
        if self._stop.is_set() or not self._connected.is_set():
            self._forget(channel)
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
                await asyncio.wait_for(pubsub.subscribe(*self._channels), self._COMMAND_TIMEOUT)
        except BaseException:
            await self._close(pubsub)
            raise
        self._pubsub = pubsub
        self._last_seen = self._last_ping = time.monotonic()
        self._reply_due = None
        if self._channels:
            self._expect_reply()
        self._connected.set()

    async def _drop(self, pubsub: PubSub) -> None:
        """Forget a broken connection; the reader will open a new one."""
        if self._pubsub is pubsub:
            self._pubsub = None
            self._connected.clear()
            if self._down_since is None:
                self._down_since = time.monotonic()
        await self._close(pubsub)

    async def _close(self, pubsub: PubSub) -> None:
        # Closing a half-open connection can block too; bound it.
        try:
            await asyncio.wait_for(pubsub.aclose(), self._COMMAND_TIMEOUT)  # type: ignore[no-untyped-call]
        except Exception:
            logger.debug("Error closing Redis pub/sub connection", exc_info=True)

    async def _run(self) -> None:
        failures = 0
        last_logged = 0.0
        while not self._stop.is_set():
            pubsub = self._pubsub
            if pubsub is None:
                try:
                    async with self._lock:
                        if self._pubsub is None:
                            await self._open()
                except Exception as exc:
                    if self._stop.is_set():
                        return
                    failures += 1
                    now = time.monotonic()
                    if failures == 1 or now - last_logged >= self._LOG_INTERVAL:
                        logger.error(
                            "Could not reconnect to Redis (attempt %d): %s; retrying",
                            failures,
                            _describe(exc),
                            exc_info=not isinstance(exc, _CONNECTION_ERRORS),
                        )
                        last_logged = now
                    delay = min(self._RECONNECT_BACKOFF_INITIAL * 2 ** (failures - 1), self._RECONNECT_BACKOFF_MAX)
                    await wait_or_stop(self._stop, delay)
                    continue
                if self._down_since is not None:
                    logger.warning(
                        "Reconnected to Redis after %.1f s; messages published meanwhile were lost",
                        time.monotonic() - self._down_since,
                    )
                    self._down_since = None
                failures = 0
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
    def __init__(self, url: str):
        url = url.replace("redis-stream", "redis", 1)
        self.streams: dict[bytes | str | memoryview, int | bytes | str | memoryview] = {}
        self._ready = asyncio.Event()
        self._producer = redis.Redis.from_url(url)
        self._consumer = redis.Redis.from_url(url)

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
        self.streams[stream.decode("utf-8")] = _msg_id.decode("utf-8")
        return Event(
            channel=stream.decode("utf-8"),
            message=message.get(b"message", b"").decode("utf-8"),
        )
