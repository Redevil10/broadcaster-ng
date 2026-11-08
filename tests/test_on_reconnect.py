from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from urllib.parse import urlsplit

import asyncpg
import pytest
import pytest_asyncio

from broadcaster import Broadcast, Event
from broadcaster.backends.memory import MemoryBackend
from broadcaster.backends.postgres import PostgresBackend
from broadcaster.backends.redis import RedisBackend

from .tcp_proxy import TcpProxy

POSTGRES_URL = os.environ.get(
    "BROADCASTER_TEST_POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/broadcaster"
)


class ReconnectingBackend(MemoryBackend):
    """A backend that can pretend to have reconnected."""

    def simulate_reconnect(self, downtime: float = 1.5) -> None:
        self._notify_reconnect(sorted(self._subscribed), downtime)


@pytest.mark.asyncio
async def test_sync_callback_receives_channels_and_downtime() -> None:
    calls: list[tuple[list[str], float]] = []
    backend = ReconnectingBackend("memory://")
    async with Broadcast(backend=backend, on_reconnect=lambda channels, downtime: calls.append((channels, downtime))):
        await asyncio.sleep(0)
        backend._subscribed.update({"b", "a"})
        backend.simulate_reconnect(2.5)
    assert calls == [(["a", "b"], 2.5)]


@pytest.mark.asyncio
async def test_async_callback_runs_in_its_own_task() -> None:
    started = asyncio.Event()
    finished = asyncio.Event()

    async def on_reconnect(channels: list[str], downtime: float) -> None:
        started.set()
        await asyncio.sleep(0.05)
        finished.set()

    backend = ReconnectingBackend("memory://")
    async with Broadcast(backend=backend, on_reconnect=on_reconnect):
        backend.simulate_reconnect()  # returns at once, does not wait for the callback
        assert not started.is_set()
        await asyncio.wait_for(finished.wait(), 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["sync", "async"])
async def test_callback_errors_are_logged_and_delivery_continues(kind: str, caplog: pytest.LogCaptureFixture) -> None:
    def sync_callback(channels: list[str], downtime: float) -> None:
        raise RuntimeError("callback failed")

    async def async_callback(channels: list[str], downtime: float) -> None:
        raise RuntimeError("callback failed")

    backend = ReconnectingBackend("memory://")
    callback = sync_callback if kind == "sync" else async_callback
    with caplog.at_level(logging.ERROR, logger="broadcaster"):
        async with Broadcast(backend=backend, on_reconnect=callback) as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                backend.simulate_reconnect()
                await asyncio.sleep(0.01)
                await broadcast.publish("chatroom", "hello")
                assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")
    assert "Error in on_reconnect callback" in caplog.text
    assert "callback failed" in caplog.text


@pytest.mark.asyncio
async def test_disconnect_cancels_pending_async_callback() -> None:
    cancelled = asyncio.Event()

    async def on_reconnect(channels: list[str], downtime: float) -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    backend = ReconnectingBackend("memory://")
    broadcast = Broadcast(backend=backend, on_reconnect=on_reconnect)
    await broadcast.connect()
    backend.simulate_reconnect()
    await asyncio.sleep(0.01)
    await asyncio.wait_for(broadcast.disconnect(), 2)
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_no_callback_without_reconnect() -> None:
    calls: list[object] = []
    async with Broadcast("memory://", on_reconnect=lambda *args: calls.append(args)) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            await asyncio.wait_for(subscriber.get(), 1)
    assert calls == []


# Real backends: the callback runs after a real reconnect, and the channel is
# already re-subscribed when it runs (a message published from inside the
# callback is delivered).


@pytest.fixture
def fast_timings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for cls in (RedisBackend, PostgresBackend):
        monkeypatch.setattr(cls, "_COMMAND_TIMEOUT", 0.5)
        monkeypatch.setattr(cls, "_RECONNECT_BACKOFF_INITIAL", 0.05)
        monkeypatch.setattr(cls, "_RECONNECT_BACKOFF_MAX", 0.2)
    yield


@pytest_asyncio.fixture
async def redis_proxy() -> AsyncIterator[TcpProxy]:
    proxy = TcpProxy("localhost", 6379)
    await proxy.start()
    yield proxy
    await proxy.stop()


async def _check_real_reconnect(url: str, cause_disconnect: Callable[[], Awaitable[None]]) -> None:
    calls: list[tuple[list[str], float]] = []
    broadcast: Broadcast

    async def on_reconnect(channels: list[str], downtime: float) -> None:
        calls.append((channels, downtime))
        await broadcast.publish("chatroom", "published from on_reconnect")

    broadcast = Broadcast(url, on_reconnect=on_reconnect)
    async with broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await cause_disconnect()
            event = await asyncio.wait_for(subscriber.get(), 10)
            assert event.message == "published from on_reconnect"
    assert len(calls) == 1
    channels, downtime = calls[0]
    assert channels == ["chatroom"]
    assert downtime >= 0


@pytest.mark.asyncio
async def test_redis_calls_on_reconnect_after_resubscribing(redis_proxy: TcpProxy, fast_timings: None) -> None:
    url = f"redis://127.0.0.1:{redis_proxy.port}?socket_timeout=0.5&socket_connect_timeout=0.5"
    await _check_real_reconnect(url, redis_proxy.reset)


@pytest.mark.asyncio
async def test_postgres_calls_on_reconnect_after_resubscribing(fast_timings: None) -> None:
    async def terminate_listener() -> None:
        admin = await asyncpg.connect(POSTGRES_URL)
        try:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1",
                PostgresBackend._APPLICATION_NAME,
            )
        finally:
            await admin.close()

    assert urlsplit(POSTGRES_URL).scheme.startswith("postgres")
    await _check_real_reconnect(POSTGRES_URL, terminate_listener)


@pytest.mark.asyncio
async def test_redis_reports_only_channels_the_reconnect_restored(monkeypatch: pytest.MonkeyPatch) -> None:
    # A channel joining while the reconnect re-subscribes is not subscribed
    # on the new connection yet, so it must not be reported as restored.
    reports: list[list[str]] = []
    backend = RedisBackend("redis://localhost:6379")
    original = RedisBackend._await_confirmations
    replaying = asyncio.Event()

    async def slow(self: RedisBackend, pubsub: object, channels: set[str]) -> None:
        replaying.set()
        await asyncio.sleep(0.5)
        await original(self, pubsub, channels)  # type: ignore[arg-type]

    async with Broadcast(backend=backend, on_reconnect=lambda channels, _: reports.append(channels)) as broadcast:
        async with broadcast.subscribe("restored"):
            monkeypatch.setattr(RedisBackend, "_await_confirmations", slow)
            async with backend._lock:
                assert backend._pubsub is not None
                await backend._drop(backend._pubsub)
            await asyncio.wait_for(replaying.wait(), 5)
            async with broadcast.subscribe("joined"):
                pass
            assert reports == [["restored"]]
