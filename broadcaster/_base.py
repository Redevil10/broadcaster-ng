from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from broadcaster._utils import stop_task, wait_or_stop

if TYPE_CHECKING:  # pragma: no cover
    from broadcaster.backends.base import BroadcastBackend

logger = logging.getLogger("broadcaster")


class Event:
    def __init__(self, channel: str, message: str) -> None:
        self.channel = channel
        self.message = message

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Event) and self.channel == other.channel and self.message == other.message

    def __repr__(self) -> str:
        return f"Event(channel={self.channel!r}, message={self.message!r})"


class Unsubscribed(Exception):
    pass


class Broadcast:
    # Listener retry policy when the backend raises: capped exponential backoff,
    # and an ERROR log on the first failure, then at most once per interval.
    _LISTENER_BACKOFF_INITIAL = 0.1
    _LISTENER_BACKOFF_MAX = 5.0
    _LISTENER_LOG_INTERVAL = 60.0
    # How long disconnect() waits for the listener to stop before closing the
    # backend anyway.
    _LISTENER_STOP_TIMEOUT = 5.0

    def __init__(
        self,
        url: str | None = None,
        *,
        backend: BroadcastBackend | None = None,
        on_reconnect: Callable[[list[str], float], Awaitable[None] | None] | None = None,
    ) -> None:
        """
        `on_reconnect(channels, downtime)` is called, if given, each time a
        backend has reconnected after losing its connection (Redis pub/sub and
        Postgres). Messages published while it was down are lost; `channels` are
        the re-subscribed channels and `downtime` the outage in seconds. It is
        called after re-subscribing, so state reloaded in it cannot miss later
        messages. It may be a plain or an async function; errors are logged.
        """
        assert url or backend, "Either `url` or `backend` must be provided."
        self._backend = backend or self._create_backend(cast(str, url))
        self._on_reconnect = on_reconnect
        self._callback_tasks: set[asyncio.Task[None]] = set()
        if on_reconnect is not None:
            self._backend._reconnect_hook = self._handle_reconnect
        self._subscribers: dict[str, set[asyncio.Queue[Event | None]]] = {}
        self._channel_locks: dict[str, _ChannelLock] = {}
        self._listener_task: asyncio.Task[None] | None = None

    def _create_backend(self, url: str) -> BroadcastBackend:
        parsed_url = urlparse(url)
        if parsed_url.scheme in ("redis", "rediss"):
            from broadcaster.backends.redis import RedisBackend

            return RedisBackend(url)

        elif parsed_url.scheme == "redis-stream":
            from broadcaster.backends.redis import RedisStreamBackend

            return RedisStreamBackend(url)

        elif parsed_url.scheme in ("postgres+psycopg", "postgresql+psycopg"):
            from broadcaster.backends.psycopg import PsycopgBackend

            return PsycopgBackend(url)

        elif parsed_url.scheme in ("postgres", "postgresql"):
            from broadcaster.backends.postgres import PostgresBackend

            return PostgresBackend(url)

        if parsed_url.scheme == "kafka":
            from broadcaster.backends.kafka import KafkaBackend

            return KafkaBackend(url)

        elif parsed_url.scheme == "memory":
            from broadcaster.backends.memory import MemoryBackend

            return MemoryBackend(url)
        raise ValueError(f"Unsupported backend: {parsed_url.scheme}")

    async def __aenter__(self) -> Broadcast:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        await self._backend.connect()
        self._listener_stop = asyncio.Event()
        self._listener_task = asyncio.create_task(self._listener(self._listener_stop))

    async def disconnect(self) -> None:
        task, self._listener_task = self._listener_task, None
        if task is not None:
            # Both signals: some backend clients swallow a cancellation that
            # arrives mid-read (seen with redis-py 8 on Python 3.11), so the
            # listener also checks the stop event after every backend call.
            self._listener_stop.set()
            done = await stop_task(task, self._LISTENER_STOP_TIMEOUT)
            if not done:
                logger.warning(
                    "Broadcast listener did not stop within %g s; disconnecting the backend anyway",
                    self._LISTENER_STOP_TIMEOUT,
                )
            elif not task.cancelled() and task.exception() is not None:
                logger.error("Broadcast listener had stopped with an error", exc_info=task.exception())
        await self._backend.disconnect()
        for callback_task in list(self._callback_tasks):
            await stop_task(callback_task, self._LISTENER_STOP_TIMEOUT)

    def _handle_reconnect(self, channels: list[str], downtime: float) -> None:
        callback = self._on_reconnect
        if callback is None:
            return
        try:
            result = callback(list(channels), downtime)
        except Exception:
            logger.exception("Error in on_reconnect callback")
            return
        if inspect.isawaitable(result):
            # Run async callbacks in their own task so a slow one does not hold
            # up the backend's reader.
            task = asyncio.ensure_future(self._await_callback(result))
            self._callback_tasks.add(task)
            task.add_done_callback(self._callback_tasks.discard)

    async def _await_callback(self, result: Awaitable[None]) -> None:
        try:
            await result
        except Exception:
            logger.exception("Error in on_reconnect callback")

    async def _listener(self, stop: asyncio.Event) -> None:
        # Supervised loop: an error from the backend must not end the listener,
        # otherwise every current and future subscriber silently gets nothing.
        # Log, back off and keep reading.
        failures = 0
        last_logged = 0.0
        while not stop.is_set():
            try:
                event = await self._backend.next_published()
            except Exception:
                if stop.is_set():
                    return
                failures += 1
                now = time.monotonic()
                if failures == 1 or now - last_logged >= self._LISTENER_LOG_INTERVAL:
                    logger.error(
                        "Error reading from the broadcast backend (%d consecutive failure%s); retrying",
                        failures,
                        "" if failures == 1 else "s",
                        exc_info=True,
                    )
                    last_logged = now
                delay = min(self._LISTENER_BACKOFF_INITIAL * 2 ** min(failures - 1, 16), self._LISTENER_BACKOFF_MAX)
                await wait_or_stop(stop, delay)
                continue

            if stop.is_set():
                return
            if failures:
                logger.warning("Broadcast backend recovered after %d consecutive failure(s)", failures)
                failures = 0
            for queue in list(self._subscribers.get(event.channel, [])):
                _offer(queue, event)

    async def publish(self, channel: str, message: Any) -> None:
        await self._backend.publish(channel, message)

    @asynccontextmanager
    async def subscribe(self, channel: str, *, max_queue_size: int | None = None) -> AsyncIterator[Subscriber]:
        """Subscribe to `channel`.

        By default the subscriber's queue is unbounded. With `max_queue_size`,
        a subscriber that does not keep up loses its oldest queued messages to
        make room for new ones (logged, and counted in
        `Subscriber.dropped_messages`); it never holds up other subscribers.
        """
        if max_queue_size is not None and max_queue_size < 1:
            raise ValueError("max_queue_size must be at least 1")
        queue = _SubscriberQueue(channel, max_queue_size)

        # If this raises (e.g. the backend is unreachable), nothing has been
        # registered, so the caller sees the backend's own error.
        await self._add_subscriber(channel, queue)
        try:
            yield Subscriber(queue)
        finally:
            await self._remove_subscriber(channel, queue)
            # Wake a reader blocked in get(); never block here on a full queue.
            _offer(queue, None)

    async def _add_subscriber(self, channel: str, queue: asyncio.Queue[Event | None]) -> None:
        # The lock makes concurrent first subscribers to a channel wait for one
        # backend subscribe instead of each subscribing and overwriting the
        # other's registration.
        async with self._channel_lock(channel):
            subscribers = self._subscribers.get(channel)
            if subscribers is not None:
                subscribers.add(queue)
                return
            # Register only after the backend subscribe succeeded.
            await self._backend.subscribe(channel)
            self._subscribers[channel] = {queue}

    async def _remove_subscriber(self, channel: str, queue: asyncio.Queue[Event | None]) -> None:
        async with self._channel_lock(channel):
            subscribers = self._subscribers.get(channel)
            if subscribers is None or queue not in subscribers:
                return
            subscribers.discard(queue)
            if subscribers:
                return
            del self._subscribers[channel]
            try:
                await self._backend.unsubscribe(channel)
            except Exception:
                # Leaving a subscription must not fail, and must not replace an
                # exception raised inside the `async with` block. The local state
                # is already cleaned up; a dead connection drops the server-side
                # subscription anyway.
                logger.warning("Failed to unsubscribe from channel %r", channel, exc_info=True)

    @asynccontextmanager
    async def _channel_lock(self, channel: str) -> AsyncIterator[None]:
        entry = self._channel_locks.get(channel)
        if entry is None:
            entry = self._channel_locks[channel] = _ChannelLock()
        entry.users += 1
        try:
            async with entry.lock:
                yield
        finally:
            entry.users -= 1
            if entry.users == 0:
                # Drop unused locks so short-lived channels do not accumulate.
                del self._channel_locks[channel]


class _ChannelLock:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


def _offer(queue: asyncio.Queue[Event | None], item: Event | None) -> None:
    if isinstance(queue, _SubscriberQueue):
        queue.offer(item)
    else:
        queue.put_nowait(item)


class _SubscriberQueue(asyncio.Queue["Event | None"]):
    """A subscriber's queue; when bounded, a full queue drops its oldest item."""

    _LOG_INTERVAL = 60.0

    def __init__(self, channel: str, max_size: int | None) -> None:
        super().__init__()  # unbounded: the size limit is enforced in offer()
        self.channel = channel
        self.max_size = max_size
        self.dropped = 0
        self._dropped_since_log = 0
        self._last_logged = 0.0

    def offer(self, item: Event | None) -> None:
        if self.max_size is not None and self.qsize() >= self.max_size:
            self.get_nowait()
            self.dropped += 1
            self._dropped_since_log += 1
            now = time.monotonic()
            if self.dropped == 1 or now - self._last_logged >= self._LOG_INTERVAL:
                logger.warning(
                    "Subscriber to channel %r is not keeping up (max_queue_size=%d): "
                    "dropped %d oldest message(s), %d in total",
                    self.channel,
                    self.max_size,
                    self._dropped_since_log,
                    self.dropped,
                )
                self._last_logged = now
                self._dropped_since_log = 0
        self.put_nowait(item)


class Subscriber:
    def __init__(self, queue: asyncio.Queue[Event | None]) -> None:
        self._queue = queue

    @property
    def dropped_messages(self) -> int:
        """Messages dropped because this subscriber's queue was full."""
        return self._queue.dropped if isinstance(self._queue, _SubscriberQueue) else 0

    async def __aiter__(self) -> AsyncGenerator[Event | None, None]:
        try:
            while True:
                yield await self.get()
        except Unsubscribed:
            pass

    async def get(self) -> Event:
        item = await self._queue.get()
        if item is None:
            raise Unsubscribed()
        return item
