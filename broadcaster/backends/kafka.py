from __future__ import annotations

import asyncio
import typing
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import IllegalStateError

from .._base import Event
from .base import BroadcastBackend


class KafkaBackend(BroadcastBackend):
    """Kafka backend: one producer, and one consumer without a consumer group.

    The consumer's partitions are assigned manually, so subscribing or
    unsubscribing one channel does not disturb the others: their read positions
    are kept. Each subscription starts at the end of the topic.
    """

    _SUBSCRIBE_TIMEOUT = 10.0  # waiting for the topic's metadata (and auto-creation)

    def __init__(self, urls: str | list[str]) -> None:
        urls = [urls] if isinstance(urls, str) else urls
        self._servers = [urlparse(url).netloc for url in urls]
        self._consumer_channels: set[str] = set()
        self._partitions: dict[str, set[TopicPartition]] = {}
        self._ready = asyncio.Event()
        # subscribe/unsubscribe change the consumer's assignment; one at a time.
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)  # pyright: ignore
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self._servers)  # pyright: ignore
        await self._producer.start()
        await self._consumer.start()

    async def disconnect(self) -> None:
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        async with self._lock:
            # partitions_for() waits for the topic's metadata. For a topic that
            # does not exist yet it also triggers auto-creation (if the broker
            # allows it), so a new topic is usable at once instead of after the
            # consumer's next periodic metadata refresh (5 minutes by default).
            try:
                partition_ids = await asyncio.wait_for(self._producer.partitions_for(channel), self._SUBSCRIBE_TIMEOUT)
            except asyncio.TimeoutError:
                raise ConnectionError(
                    f"Kafka: no metadata for topic {channel!r} within {self._SUBSCRIBE_TIMEOUT:g} s "
                    "(does the topic exist, or is topic auto-creation enabled?)"
                ) from None
            new = {TopicPartition(channel, partition) for partition in partition_ids}
            self._partitions[channel] = new
            self._consumer_channels.add(channel)
            await self._reassign(new)
            self._ready.set()

    async def unsubscribe(self, channel: str) -> None:
        # Unsubscribe only this channel (encode/broadcaster#65): upstream called
        # the consumer's unsubscribe(), which dropped every topic.
        async with self._lock:
            self._consumer_channels.discard(channel)
            self._partitions.pop(channel, None)
            if self._consumer_channels:
                await self._reassign(set())
            else:
                self._ready.clear()
                self._consumer.unsubscribe()

    async def _reassign(self, new: set[TopicPartition]) -> None:
        """Assign the partitions of every subscribed channel.

        assign() resets every partition's read position, so the positions of
        the partitions that stay assigned are saved first and restored after.
        New partitions start at the end of the topic, resolved now so that
        messages published after subscribe() returns are delivered.
        """
        wanted = set().union(*self._partitions.values())
        kept = (wanted - new) & self._consumer.assignment()
        positions = {tp: await self._consumer.position(tp) for tp in kept}
        self._consumer.assign(list(wanted))
        for tp, offset in positions.items():
            self._consumer.seek(tp, offset)
        if new:
            await self._consumer.seek_to_end(*new)
            for tp in new:
                await self._consumer.position(tp)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def next_published(self) -> Event:
        while True:
            await self._ready.wait()
            try:
                message = await self._consumer.getone()
                break
            except IllegalStateError:
                # Unsubscribed from the last topic meanwhile: wait for the next.
                if self._consumer_channels:
                    raise
        value = message.value

        # for type compatibility:
        # we declare Event.message as str, so convert None to empty string
        if value is None:
            value = b""
        return Event(channel=message.topic, message=value.decode("utf8"))
