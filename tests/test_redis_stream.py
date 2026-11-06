"""Redis Streams backend: connection loss and unsubscribe behaviour."""

from __future__ import annotations

import asyncio
import logging
import typing
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from redis import asyncio as redis

from broadcaster import Broadcast
from broadcaster._base import Subscriber
from broadcaster.backends.redis import RedisStreamBackend

from .tcp_proxy import TcpProxy


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_INITIAL", 0.05)
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_MAX", 0.2)
    yield


@pytest_asyncio.fixture
async def proxy() -> AsyncIterator[TcpProxy]:
    proxy = TcpProxy("localhost", 6379)
    await proxy.start()
    yield proxy
    await proxy.stop()


@pytest_asyncio.fixture
async def direct() -> AsyncIterator[redis.Redis]:
    """A client that bypasses the proxy, to publish while the proxy is broken."""
    client = redis.Redis.from_url("redis://localhost:6379")
    yield client
    await client.aclose()


def proxied_url(proxy: TcpProxy) -> str:
    return f"redis-stream://127.0.0.1:{proxy.port}?socket_timeout=0.5&socket_connect_timeout=0.5"


def new_stream() -> str:
    return f"stream_{uuid.uuid4().hex[:12]}"


async def receive(subscriber: Subscriber, count: int, timeout: float = 10.0) -> list[str]:
    return [(await asyncio.wait_for(subscriber.get(), timeout)).message for _ in range(count)]


@pytest.mark.asyncio
async def test_messages_published_during_outage_are_delivered_after_reconnect(
    proxy: TcpProxy, direct: redis.Redis
) -> None:
    # Unlike pub/sub, a stream stores messages: the backend continues from the
    # last ID it read, so nothing published during the outage is lost.
    stream = new_stream()
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe(stream) as subscriber:
            await broadcast.publish(stream, "before")
            assert await receive(subscriber, 1) == ["before"]
            await proxy.refuse()
            await proxy.reset()
            for n in range(3):
                await direct.xadd(stream, {"message": f"during-{n}"})
            await asyncio.sleep(0.3)
            await proxy.accept()
            assert await receive(subscriber, 3) == ["during-0", "during-1", "during-2"]


@pytest.mark.asyncio
async def test_half_open_link_times_out_and_recovers(
    proxy: TcpProxy, direct: redis.Redis, caplog: pytest.LogCaptureFixture
) -> None:
    # Without a socket timeout, XREAD on a half-open connection waits forever.
    stream = new_stream()
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe(stream) as subscriber:
                await broadcast.publish(stream, "before")
                assert await receive(subscriber, 1) == ["before"]
                proxy.mute()
                await direct.xadd(stream, {"message": "during"})
                await asyncio.sleep(1.0)  # longer than socket_timeout
                assert "Error reading from the broadcast backend" in caplog.text
                proxy.unmute()
                await proxy.reset()  # drop the muted connections
                assert await receive(subscriber, 1) == ["during"]


@pytest.mark.asyncio
async def test_message_in_flight_does_not_resurrect_unsubscribed_stream() -> None:
    # next_published() used to write the stream's last ID back even if the
    # channel was unsubscribed meanwhile, so the backend kept reading it.
    backend = RedisStreamBackend("redis-stream://localhost:6379")
    stream = new_stream()
    await backend.connect()
    try:
        await backend.subscribe(stream)
        await backend.publish(stream, "hello")
        original = backend.wait_for_messages

        async def unsubscribe_while_reading() -> typing.Any:
            messages = await original()
            await backend.unsubscribe(stream)  # arrives while the message is in flight
            return messages

        backend.wait_for_messages = unsubscribe_while_reading  # type: ignore[method-assign]
        event = await asyncio.wait_for(backend.next_published(), 5)
        assert event.message == "hello"
        assert stream not in backend.streams
    finally:
        await backend.disconnect()


@pytest.mark.asyncio
async def test_default_socket_timeouts() -> None:
    backend = RedisStreamBackend("redis-stream://localhost:6379")
    kwargs = backend._consumer.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == RedisStreamBackend._DEFAULT_SOCKET_TIMEOUT
    assert kwargs["socket_connect_timeout"] == RedisStreamBackend._DEFAULT_SOCKET_CONNECT_TIMEOUT
    await backend.disconnect()

    # The URL overrides the defaults.
    backend = RedisStreamBackend("redis-stream://localhost:6379?socket_timeout=9")
    assert backend._consumer.connection_pool.connection_kwargs["socket_timeout"] == 9.0
    await backend.disconnect()
