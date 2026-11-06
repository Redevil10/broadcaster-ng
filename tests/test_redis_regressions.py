from __future__ import annotations

import asyncio
import typing

import pytest

from broadcaster import Broadcast
from broadcaster.backends.redis import RedisBackend


@pytest.mark.asyncio
async def test_redis_subscribe_that_yields_still_delivers() -> None:
    # Regression: the listener was woken before redis-py registered the channel.
    # If the SUBSCRIBE call yielded to the event loop, `PubSub.listen()` saw no
    # subscriptions, returned at once, and the listener then waited forever, so
    # no message was ever delivered. Whether the call yields depends on the
    # redis-py / Python versions, so force it here.
    backend = RedisBackend("redis://localhost:6379")
    original = backend._pubsub.execute_command

    async def yielding_execute_command(*args: typing.Any) -> typing.Any:
        await asyncio.sleep(0)
        return await original(*args)

    backend._pubsub.execute_command = yielding_execute_command  # type: ignore[method-assign]

    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await asyncio.wait_for(subscriber.get(), timeout=5)
            assert event.channel == "chatroom"
            assert event.message == "hello"
