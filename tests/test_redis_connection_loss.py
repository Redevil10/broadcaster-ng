"""Redis pub/sub backend: reconnect, timeouts and half-open detection.

Uses a real Redis behind a TCP proxy that can reset, refuse or mute connections.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from redis import asyncio as redis

from broadcaster import Broadcast, Event
from broadcaster._base import Subscriber
from broadcaster.backends.redis import RedisBackend

from .tcp_proxy import TcpProxy

REDIS_HOST = "localhost"
REDIS_PORT = 6379
SILENT_LIMIT = 0.6


@pytest.fixture(autouse=True)
def fast_timings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name, value in {
        "_READ_TIMEOUT": 0.05,
        "_PING_AFTER": 0.2,
        "_SILENT_LIMIT": SILENT_LIMIT,
        "_COMMAND_TIMEOUT": 0.5,
        "_SUBSCRIBE_TIMEOUT": 3.0,
        "_RECONNECT_BACKOFF_INITIAL": 0.05,
        "_RECONNECT_BACKOFF_MAX": 0.2,
        "_STOP_TIMEOUT": 1.0,
    }.items():
        monkeypatch.setattr(RedisBackend, name, value)
    yield


@pytest_asyncio.fixture
async def proxy() -> AsyncIterator[TcpProxy]:
    proxy = TcpProxy(REDIS_HOST, REDIS_PORT)
    await proxy.start()
    yield proxy
    await proxy.stop()


def proxied_url(proxy: TcpProxy) -> str:
    return f"redis://127.0.0.1:{proxy.port}?socket_timeout=0.5&socket_connect_timeout=0.5"


async def publish_until_received(
    broadcast: Broadcast, subscriber: Subscriber, channel: str = "chatroom", timeout: float = 10.0
) -> Event:
    """Publish repeatedly until the subscriber gets a message.

    Messages published while the backend is reconnecting are lost (pub/sub is
    at-most-once), so a single publish right after a failure may not arrive.
    """
    deadline = time.monotonic() + timeout
    n = 0
    while time.monotonic() < deadline:
        n += 1
        try:
            await broadcast.publish(channel, f"probe-{n}")
        except Exception:
            pass  # the publish connection may be broken too
        try:
            return await asyncio.wait_for(subscriber.get(), 0.1)
        except asyncio.TimeoutError:
            continue
    raise AssertionError(f"no message received within {timeout} s")


@pytest.mark.asyncio
async def test_reconnects_after_client_kill(caplog: pytest.LogCaptureFixture) -> None:
    killer = redis.Redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
    try:
        with caplog.at_level(logging.WARNING, logger="broadcaster"):
            async with Broadcast(f"redis://{REDIS_HOST}:{REDIS_PORT}") as broadcast:
                async with broadcast.subscribe("chatroom") as subscriber:
                    await publish_until_received(broadcast, subscriber)
                    await killer.client_kill_filter(_type="pubsub")
                    await publish_until_received(broadcast, subscriber)
    finally:
        await killer.aclose()
    assert "Lost connection to Redis" in caplog.text
    assert "Reconnected to Redis" in caplog.text


@pytest.mark.asyncio
async def test_reconnects_after_connection_reset(proxy: TcpProxy) -> None:
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await publish_until_received(broadcast, subscriber)
            accepted = proxy.connections_accepted
            await proxy.reset()
            await publish_until_received(broadcast, subscriber)
            assert proxy.connections_accepted > accepted  # really a new connection


@pytest.mark.asyncio
async def test_reconnects_after_outage(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await proxy.refuse()
                await proxy.reset()
                await asyncio.sleep(0.5)  # several failed reconnect attempts
                await proxy.accept()
                await publish_until_received(broadcast, subscriber)
    assert "Could not reconnect to Redis (attempt 1)" in caplog.text
    assert "Reconnected to Redis after" in caplog.text
    # Repeated failures are not logged one by one.
    assert caplog.text.count("Could not reconnect to Redis") == 1


@pytest.mark.asyncio
async def test_reconnect_to_server_that_never_answers_times_out_and_retries(proxy: TcpProxy) -> None:
    # Fix #4: without a socket timeout the reconnect handshake waits forever.
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            proxy.mute()
            await proxy.reset()  # the reconnect now reaches a server that never answers
            await asyncio.sleep(1.5)  # longer than the connect/command timeouts
            accepted = proxy.connections_accepted
            proxy.unmute()
            await publish_until_received(broadcast, subscriber)
            assert proxy.connections_accepted > accepted  # kept retrying


@pytest.mark.asyncio
async def test_half_open_connection_is_detected(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # Fix #5: the connection stays open but nothing gets through any more.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
                proxy.mute()
                await asyncio.sleep(SILENT_LIMIT + 0.5)
                assert "no data from Redis" in caplog.text
                proxy.unmute()
                await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_idle_healthy_connection_is_kept(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # The other direction of #5: PING/PONG must keep an idle connection alive.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
                accepted = proxy.connections_accepted
                await asyncio.sleep(SILENT_LIMIT * 4)  # idle, no messages at all
                assert proxy.connections_accepted == accepted
                await publish_until_received(broadcast, subscriber)
    assert "Lost connection" not in caplog.text


@pytest.mark.asyncio
async def test_subscribe_during_short_outage_completes_after_reconnect(proxy: TcpProxy) -> None:
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("other"):
            await proxy.refuse()
            await proxy.reset()

            async def subscribe_and_receive() -> Event:
                async with broadcast.subscribe("chatroom") as subscriber:
                    return await publish_until_received(broadcast, subscriber)

            task = asyncio.create_task(subscribe_and_receive())
            await asyncio.sleep(0.5)
            assert not task.done()  # waiting for Redis to come back
            await proxy.accept()
            event = await asyncio.wait_for(task, 10)
            assert event.channel == "chatroom"


@pytest.mark.asyncio
async def test_subscribe_during_long_outage_raises(proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(RedisBackend, "_SUBSCRIBE_TIMEOUT", 0.5)
    async with Broadcast(proxied_url(proxy)) as broadcast:
        async with broadcast.subscribe("other"):
            await proxy.refuse()
            await proxy.reset()
            await asyncio.sleep(0.1)
            with pytest.raises(ConnectionError, match="Redis is unavailable"):
                async with broadcast.subscribe("chatroom"):
                    pass  # pragma: no cover
            assert "chatroom" not in broadcast._subscribers
            backend = broadcast._backend
            assert isinstance(backend, RedisBackend)
            assert "chatroom" not in backend._channels

            # Once Redis is back, subscribing works again.
            await proxy.accept()
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_disconnect_during_outage_is_prompt(proxy: TcpProxy) -> None:
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        await proxy.refuse()
        await proxy.reset()
        await asyncio.sleep(0.3)
    await asyncio.wait_for(broadcast.disconnect(), 3)


@pytest.mark.asyncio
async def test_disconnect_while_half_open_is_prompt(proxy: TcpProxy) -> None:
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        proxy.mute()
        await asyncio.wait_for(broadcast.disconnect(), 3)


@pytest.mark.asyncio
async def test_resubscribe_after_unsubscribing_everything(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # With no channels the reader cannot read (redis-py raises); it must wait
    # and not treat the quiet connection as dead.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
            await asyncio.sleep(SILENT_LIMIT * 2)
            async with broadcast.subscribe("chatroom") as subscriber:
                await publish_until_received(broadcast, subscriber)
    assert "Lost connection" not in caplog.text


@pytest.mark.asyncio
async def test_connect_fails_fast_when_redis_is_unreachable(proxy: TcpProxy) -> None:
    await proxy.refuse()
    with pytest.raises(Exception):
        await asyncio.wait_for(Broadcast(proxied_url(proxy)).connect(), 3)


@pytest.mark.asyncio
async def test_decode_responses_client() -> None:
    # A preconfigured client may return str instead of bytes.
    conn = redis.Redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    async with Broadcast(backend=RedisBackend(conn=conn)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            event = await publish_until_received(broadcast, subscriber)
            assert event.channel == "chatroom"


@pytest.mark.asyncio
async def test_disconnect_while_reconnecting_to_unresponsive_server_is_prompt(proxy: TcpProxy) -> None:
    # Fix #9a: the reader is stuck in a connect handshake that never completes.
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("chatroom"):
        proxy.mute()
        await proxy.reset()
        await asyncio.sleep(0.1)  # reader is now reconnecting into the muted proxy
        started = time.monotonic()
        await asyncio.wait_for(broadcast.disconnect(), 3)
        assert time.monotonic() - started < 1.0


@pytest.mark.asyncio
async def test_disconnect_fails_pending_subscribe_promptly(proxy: TcpProxy) -> None:
    # Fix #9a: a subscribe() waiting for the reconnect must not wait out its
    # full timeout once the backend is disconnecting.
    broadcast = Broadcast(proxied_url(proxy))
    await broadcast.connect()
    async with broadcast.subscribe("other"):
        await proxy.refuse()
        await proxy.reset()
        await asyncio.sleep(0.1)

        async def subscribe() -> None:
            async with broadcast.subscribe("chatroom"):
                pass  # pragma: no cover

        pending = asyncio.create_task(subscribe())
        await asyncio.sleep(0.2)
        assert not pending.done()
        started = time.monotonic()
        await asyncio.wait_for(broadcast._backend.disconnect(), 3)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(pending, 1)
        assert time.monotonic() - started < 1.5


@pytest.mark.asyncio
async def test_subscribe_on_silently_dead_connection_is_detected_quickly(
    proxy: TcpProxy, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # SUBSCRIBE does not wait for the server's reply. On a silently dead
    # connection the missing reply must be noticed within the command deadline
    # (0.5 s here), not only after the much longer silence limit.
    monkeypatch.setattr(RedisBackend, "_SILENT_LIMIT", 30.0)
    monkeypatch.setattr(RedisBackend, "_PING_AFTER", 30.0)
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("other") as other:
                await publish_until_received(broadcast, other, "other")
                proxy.mute()
                async with broadcast.subscribe("chatroom") as subscriber:
                    await asyncio.sleep(1.0)
                    assert "no reply from Redis" in caplog.text
                    proxy.unmute()
                    await publish_until_received(broadcast, subscriber)


@pytest.mark.asyncio
async def test_leaving_subscription_on_dead_connection(proxy: TcpProxy, caplog: pytest.LogCaptureFixture) -> None:
    # UNSUBSCRIBE does not wait for the reply, so leaving is immediate even on a
    # dead connection; the missing reply then reveals the dead connection and
    # the remaining subscriber recovers after the reconnect.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(proxied_url(proxy)) as broadcast:
            async with broadcast.subscribe("other") as other:
                await publish_until_received(broadcast, other, "other")
                async with broadcast.subscribe("chatroom"):
                    proxy.mute()
                    started = time.monotonic()
                assert time.monotonic() - started < 0.5
                await asyncio.sleep(1.0)
                assert "no reply from Redis" in caplog.text
                proxy.unmute()
                await publish_until_received(broadcast, other, "other")
