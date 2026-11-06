from __future__ import annotations

import asyncio
import logging
import typing

import pytest
from redis.asyncio.client import PubSub

from broadcaster import Broadcast


@pytest.mark.asyncio
async def test_redis_subscribe_that_yields_still_delivers(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression: the listener was woken before redis-py registered the channel.
    # If the SUBSCRIBE call yielded to the event loop, `PubSub.listen()` saw no
    # subscriptions, returned at once, and the listener then waited forever, so
    # no message was ever delivered. Whether the call yields depends on the
    # redis-py / Python versions, so force it here.
    original = PubSub.execute_command

    async def yielding_execute_command(self: PubSub, *args: typing.Any) -> typing.Any:
        await asyncio.sleep(0)
        return await original(self, *args)

    monkeypatch.setattr(PubSub, "execute_command", yielding_execute_command)

    async with Broadcast("redis://localhost:6379") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await asyncio.wait_for(subscriber.get(), timeout=5)
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_stream_resubscribe_after_last_unsubscribe(caplog: pytest.LogCaptureFixture) -> None:
    # Regression (encode/broadcaster#149): after the last unsubscribe the
    # backend called XREAD with no streams, which raises DataError. Before the
    # supervised listener this silently killed message delivery.
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast("redis-stream://localhost:6379") as broadcast:
            async with broadcast.subscribe("chatroom") as subscriber:
                await broadcast.publish("chatroom", "first")
                assert (await asyncio.wait_for(subscriber.get(), 5)).message == "first"
            await asyncio.sleep(0.3)  # longer than one XREAD block (100 ms)
            async with broadcast.subscribe("chatroom") as subscriber:
                await broadcast.publish("chatroom", "second")
                assert (await asyncio.wait_for(subscriber.get(), 5)).message == "second"
    assert caplog.text == ""
