from __future__ import annotations

import asyncio
import logging

import pytest

from broadcaster import Broadcast, Event
from broadcaster.backends.memory import MemoryBackend


class ControllableBackend(MemoryBackend):
    """Memory backend whose subscribe/unsubscribe can be made slow or failing."""

    def __init__(self, url: str = "memory://") -> None:
        super().__init__(url)
        self.subscribe_error: Exception | None = None
        self.unsubscribe_error: Exception | None = None
        self.delay = 0.0
        self.calls: list[tuple[str, str]] = []

    async def subscribe(self, channel: str) -> None:
        self.calls.append(("subscribe", channel))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.subscribe_error is not None:
            raise self.subscribe_error
        await super().subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        self.calls.append(("unsubscribe", channel))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.unsubscribe_error is not None:
            raise self.unsubscribe_error
        await super().unsubscribe(channel)


@pytest.mark.asyncio
async def test_subscribe_error_is_not_hidden() -> None:
    backend = ControllableBackend()
    backend.subscribe_error = ConnectionError("backend is down")
    async with Broadcast(backend=backend) as broadcast:
        with pytest.raises(ConnectionError, match="backend is down"):
            async with broadcast.subscribe("chatroom"):
                pass  # pragma: no cover


@pytest.mark.asyncio
async def test_failed_subscribe_leaves_no_state_and_can_be_retried() -> None:
    backend = ControllableBackend()
    backend.subscribe_error = ConnectionError("backend is down")
    async with Broadcast(backend=backend) as broadcast:
        with pytest.raises(ConnectionError):
            async with broadcast.subscribe("chatroom"):
                pass  # pragma: no cover
        assert broadcast._subscribers == {}
        assert broadcast._channel_locks == {}

        backend.subscribe_error = None
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")


@pytest.mark.asyncio
async def test_cancelled_subscribe_leaves_no_state() -> None:
    backend = ControllableBackend()
    backend.delay = 10
    async with Broadcast(backend=backend) as broadcast:

        async def subscribe() -> None:
            async with broadcast.subscribe("chatroom"):
                pass  # pragma: no cover

        task = asyncio.create_task(subscribe())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert broadcast._subscribers == {}
        assert broadcast._channel_locks == {}


@pytest.mark.asyncio
async def test_concurrent_first_subscribers_all_receive() -> None:
    # Two subscribers to a new channel at the same time must share one backend
    # subscribe; previously the second overwrote the first's registration.
    backend = ControllableBackend()
    backend.delay = 0.01
    async with Broadcast(backend=backend) as broadcast:
        joined = asyncio.Event()
        received: list[Event] = []

        async def subscriber() -> None:
            async with broadcast.subscribe("chatroom") as sub:
                if len(broadcast._subscribers.get("chatroom", ())) == 2:
                    joined.set()
                received.append(await asyncio.wait_for(sub.get(), 1))

        tasks = [asyncio.create_task(subscriber()) for _ in range(2)]
        await asyncio.wait_for(joined.wait(), 1)
        await broadcast.publish("chatroom", "hello")
        await asyncio.gather(*tasks)

        assert received == [Event("chatroom", "hello")] * 2
        assert backend.calls == [("subscribe", "chatroom"), ("unsubscribe", "chatroom")]
        assert broadcast._subscribers == {}
        assert broadcast._channel_locks == {}


@pytest.mark.asyncio
async def test_subscribe_during_last_unsubscribe_resubscribes() -> None:
    # A new subscriber arriving while the last one is still unsubscribing must
    # end up subscribed on the backend: its subscribe has to run after that
    # unsubscribe, not before it.
    backend = ControllableBackend()
    async with Broadcast(backend=backend) as broadcast:
        entered = asyncio.Event()
        leave = asyncio.Event()

        async def first() -> None:
            async with broadcast.subscribe("chatroom"):
                entered.set()
                await leave.wait()

        first_task = asyncio.create_task(first())
        await entered.wait()
        backend.delay = 0.05
        leave.set()
        await asyncio.sleep(0.01)
        assert backend.calls[-1] == ("unsubscribe", "chatroom")  # still in flight

        backend.delay = 0
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            assert await asyncio.wait_for(subscriber.get(), 1) == Event("chatroom", "hello")
        await first_task

        assert backend.calls == [
            ("subscribe", "chatroom"),
            ("unsubscribe", "chatroom"),
            ("subscribe", "chatroom"),
            ("unsubscribe", "chatroom"),
        ]


@pytest.mark.asyncio
async def test_unsubscribe_error_does_not_mask_body_exception(caplog: pytest.LogCaptureFixture) -> None:
    backend = ControllableBackend()
    backend.unsubscribe_error = ConnectionError("backend is down")
    async with Broadcast(backend=backend) as broadcast:
        with caplog.at_level(logging.WARNING, logger="broadcaster"):
            with pytest.raises(RuntimeError, match="app error"):
                async with broadcast.subscribe("chatroom"):
                    raise RuntimeError("app error")
        assert broadcast._subscribers == {}
        assert "Failed to unsubscribe from channel 'chatroom'" in caplog.text


@pytest.mark.asyncio
async def test_unsubscribe_error_on_normal_exit_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    backend = ControllableBackend()
    backend.unsubscribe_error = ConnectionError("backend is down")
    async with Broadcast(backend=backend) as broadcast:
        with caplog.at_level(logging.WARNING, logger="broadcaster"):
            async with broadcast.subscribe("chatroom"):
                pass
        assert broadcast._subscribers == {}
        assert broadcast._channel_locks == {}
        assert "Failed to unsubscribe from channel 'chatroom'" in caplog.text
