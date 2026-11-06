from __future__ import annotations

import asyncio
import logging

import pytest

from broadcaster import Broadcast


async def flush() -> None:
    """Let the broadcast listener deliver everything published so far."""
    for _ in range(20):
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_full_queue_drops_oldest(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast("memory://") as broadcast:
            async with broadcast.subscribe("chatroom", max_queue_size=3) as subscriber:
                for n in range(10):
                    await broadcast.publish("chatroom", str(n))
                await flush()
                received = [(await asyncio.wait_for(subscriber.get(), 1)).message for _ in range(3)]
                assert received == ["7", "8", "9"]
                assert subscriber.dropped_messages == 7
    # Logged once (the first drop), not once per dropped message.
    warnings = [r for r in caplog.records if "not keeping up" in r.getMessage()]
    assert len(warnings) == 1
    assert "max_queue_size=3" in warnings[0].getMessage()


@pytest.mark.asyncio
async def test_unbounded_by_default() -> None:
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            for n in range(500):
                await broadcast.publish("chatroom", str(n))
            await flush()
            received = [(await asyncio.wait_for(subscriber.get(), 1)).message for _ in range(500)]
            assert received == [str(n) for n in range(500)]
            assert subscriber.dropped_messages == 0


@pytest.mark.asyncio
async def test_slow_subscriber_does_not_hold_up_others() -> None:
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom", max_queue_size=2) as slow:
            async with broadcast.subscribe("chatroom") as fast:
                for n in range(50):
                    await broadcast.publish("chatroom", str(n))
                received = [(await asyncio.wait_for(fast.get(), 1)).message for _ in range(50)]
                assert received == [str(n) for n in range(50)]
                assert slow.dropped_messages == 48


@pytest.mark.asyncio
async def test_leaving_with_a_full_queue_does_not_block() -> None:
    async with Broadcast("memory://") as broadcast:

        async def subscribe_and_leave() -> None:
            async with broadcast.subscribe("chatroom", max_queue_size=1):
                await broadcast.publish("chatroom", "a")
                await broadcast.publish("chatroom", "b")
                await flush()

        await asyncio.wait_for(subscribe_and_leave(), 1)
        assert broadcast._subscribers == {}


@pytest.mark.asyncio
async def test_iteration_ends_after_leaving_with_a_full_queue() -> None:
    async with Broadcast("memory://") as broadcast:
        received: list[str] = []
        async with broadcast.subscribe("chatroom", max_queue_size=2) as subscriber:
            for n in range(5):
                await broadcast.publish("chatroom", str(n))
            await flush()

            async def consume() -> None:
                async for event in subscriber:
                    assert event is not None
                    received.append(event.message)

            consumer = asyncio.create_task(consume())
            await flush()
        await asyncio.wait_for(consumer, 1)
        assert received == ["3", "4"]


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [0, -1])
async def test_invalid_max_queue_size(size: int) -> None:
    async with Broadcast("memory://") as broadcast:
        with pytest.raises(ValueError, match="max_queue_size"):
            async with broadcast.subscribe("chatroom", max_queue_size=size):
                pass  # pragma: no cover
