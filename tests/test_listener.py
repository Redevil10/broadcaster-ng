from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterator

import pytest

from broadcaster import Broadcast, Event
from broadcaster.backends.memory import MemoryBackend


class FailingBackend(MemoryBackend):
    """Memory backend whose next_published() raises a set number of times."""

    def __init__(self, url: str = "memory://") -> None:
        super().__init__(url)
        self.failures_left = 0
        self.calls = 0

    async def next_published(self) -> Event:
        self.calls += 1
        if self.failures_left:
            self.failures_left -= 1
            await asyncio.sleep(0)
            raise ConnectionError("connection lost")
        return await super().next_published()


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_INITIAL", 0.001)
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_MAX", 0.002)
    yield


@pytest.mark.asyncio
async def test_existing_subscriber_keeps_receiving_after_backend_error(caplog: pytest.LogCaptureFixture) -> None:
    backend = FailingBackend()
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            with caplog.at_level(logging.WARNING, logger="broadcaster"):
                backend.failures_left = 1
                # The listener is already waiting in next_published(); this message
                # wakes it, and its next call fails.
                await broadcast.publish("chatroom", "lost?")
                await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 1)

                await broadcast.publish("chatroom", "hello")
                event = await asyncio.wait_for(subscriber.get(), 1)
                while event.message != "hello":
                    event = await asyncio.wait_for(subscriber.get(), 1)

            assert broadcast._listener_task is not None
            assert not broadcast._listener_task.done()
            assert "Error reading from the broadcast backend (1 consecutive failure)" in caplog.text
            assert "recovered after 1 consecutive failure(s)" in caplog.text


@pytest.mark.asyncio
async def test_new_subscriber_works_after_backend_error() -> None:
    backend = FailingBackend()
    backend.failures_left = 3
    async with Broadcast(backend=backend) as broadcast:
        await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 1)
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")


@pytest.mark.asyncio
async def test_repeated_failures_are_rate_limited_in_the_log(caplog: pytest.LogCaptureFixture) -> None:
    backend = FailingBackend()
    backend.failures_left = 20
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(backend=backend) as broadcast:
            await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 1)
            # Recovery is logged when the backend next returns a message.
            async with broadcast.subscribe("chatroom") as subscriber:
                await broadcast.publish("chatroom", "hello")
                assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1  # only the first failure within the log interval
    assert "recovered after 20 consecutive failure(s)" in caplog.text


@pytest.mark.asyncio
async def test_disconnect_does_not_raise_after_backend_error() -> None:
    # Upstream re-raised the listener's error from disconnect(); now the listener
    # never dies from a backend error, and disconnect() just stops it.
    backend = FailingBackend()
    backend.failures_left = 1
    broadcast = Broadcast(backend=backend)
    await broadcast.connect()
    await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 1)
    task = broadcast._listener_task
    await broadcast.disconnect()
    assert task is not None and task.done()
    assert broadcast._listener_task is None


@pytest.mark.asyncio
async def test_disconnect_during_backoff_returns_promptly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_INITIAL", 60.0)
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_MAX", 60.0)
    backend = FailingBackend()
    backend.failures_left = 1
    broadcast = Broadcast(backend=backend)
    await broadcast.connect()
    await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 1)
    # The listener is now sleeping for 60 s; disconnect must not wait for it.
    await asyncio.wait_for(broadcast.disconnect(), 1)


class SwallowsCancelBackend(MemoryBackend):
    """next_published() turns a cancellation into an ordinary error, like some
    client libraries do when the cancel arrives mid-read."""

    async def next_published(self) -> Event:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            raise ConnectionError("read interrupted") from None
        raise AssertionError("unreachable")  # pragma: no cover


class IgnoresCancelBackend(MemoryBackend):
    """next_published() ignores cancellation until the backend is disconnected."""

    def __init__(self, url: str = "memory://") -> None:
        super().__init__(url)
        self.closed = asyncio.Event()

    async def next_published(self) -> Event:
        while not self.closed.is_set():
            try:
                await self.closed.wait()
            except asyncio.CancelledError:
                pass
        raise ConnectionError("backend closed")

    async def disconnect(self) -> None:
        self.closed.set()


@pytest.mark.asyncio
async def test_disconnect_when_backend_swallows_cancellation(caplog: pytest.LogCaptureFixture) -> None:
    broadcast = Broadcast(backend=SwallowsCancelBackend("memory://"))
    await broadcast.connect()
    await asyncio.sleep(0.01)
    task = broadcast._listener_task
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        await asyncio.wait_for(broadcast.disconnect(), 1)
    assert task is not None and task.done()
    assert caplog.text == ""  # a shutdown is not reported as a backend error


@pytest.mark.asyncio
async def test_disconnect_when_backend_ignores_cancellation(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(Broadcast, "_LISTENER_STOP_TIMEOUT", 0.05)
    backend = IgnoresCancelBackend()
    broadcast = Broadcast(backend=backend)
    await broadcast.connect()
    await asyncio.sleep(0.01)
    task = broadcast._listener_task
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        await asyncio.wait_for(broadcast.disconnect(), 1)
    assert "did not stop within" in caplog.text
    # Closing the backend made next_published() fail; the listener then stops.
    assert task is not None
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_disconnect_without_connect() -> None:
    await Broadcast("memory://").disconnect()


async def _until(predicate: Callable[[], bool]) -> None:
    while not predicate():
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_listener_survives_a_very_long_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    # The backoff used to compute INITIAL * 2 ** (failures - 1), which raises
    # OverflowError at failure 1025 and ended the listener for good.
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_INITIAL", 0.0)
    monkeypatch.setattr(Broadcast, "_LISTENER_BACKOFF_MAX", 0.0)
    backend = FailingBackend()
    backend.failures_left = 1100
    async with Broadcast(backend=backend) as broadcast:
        await asyncio.wait_for(_until(lambda: backend.failures_left == 0), 10)
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")
