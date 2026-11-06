from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

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
    def __init__(self, url: str | None = None, *, backend: BroadcastBackend | None = None) -> None:
        assert url or backend, "Either `url` or `backend` must be provided."
        self._backend = backend or self._create_backend(cast(str, url))
        self._subscribers: dict[str, set[asyncio.Queue[Event | None]]] = {}
        self._channel_locks: dict[str, _ChannelLock] = {}

    def _create_backend(self, url: str) -> BroadcastBackend:
        parsed_url = urlparse(url)
        if parsed_url.scheme in ("redis", "rediss"):
            from broadcaster.backends.redis import RedisBackend

            return RedisBackend(url)

        elif parsed_url.scheme == "redis-stream":
            from broadcaster.backends.redis import RedisStreamBackend

            return RedisStreamBackend(url)

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
        self._listener_task = asyncio.create_task(self._listener())

    async def disconnect(self) -> None:
        if self._listener_task.done():
            self._listener_task.result()
        else:
            self._listener_task.cancel()
        await self._backend.disconnect()

    async def _listener(self) -> None:
        while True:
            event = await self._backend.next_published()
            for queue in list(self._subscribers.get(event.channel, [])):
                await queue.put(event)

    async def publish(self, channel: str, message: Any) -> None:
        await self._backend.publish(channel, message)

    @asynccontextmanager
    async def subscribe(self, channel: str) -> AsyncIterator[Subscriber]:
        queue: asyncio.Queue[Event | None] = asyncio.Queue()

        # If this raises (e.g. the backend is unreachable), nothing has been
        # registered, so the caller sees the backend's own error.
        await self._add_subscriber(channel, queue)
        try:
            yield Subscriber(queue)
        finally:
            await self._remove_subscriber(channel, queue)
            await queue.put(None)

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


class Subscriber:
    def __init__(self, queue: asyncio.Queue[Event | None]) -> None:
        self._queue = queue

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
