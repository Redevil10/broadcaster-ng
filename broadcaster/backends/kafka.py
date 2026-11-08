from __future__ import annotations

import asyncio
import logging
import typing
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.errors import IllegalStateError

from .._base import Event
from .._utils import stop_task, wait_or_stop
from .base import BroadcastBackend

logger = logging.getLogger("broadcaster.kafka")


class KafkaBackend(BroadcastBackend):
    """Kafka backend: one producer, and one consumer without a consumer group.

    The consumer's partitions are assigned manually, so subscribing or
    unsubscribing one channel does not disturb the others: their read positions
    are kept. Each subscription starts at the end of the topic. Partitions
    added to a subscribed topic later are picked up by a periodic check and
    read from their beginning.
    """

    _SUBSCRIBE_TIMEOUT = 10.0  # topic metadata (and auto-creation), then the start offsets
    _PARTITION_REFRESH_INTERVAL = 60.0  # how often to look for partitions added to subscribed topics

    def __init__(self, urls: str | list[str]) -> None:
        urls = [urls] if isinstance(urls, str) else urls
        self._servers = [urlparse(url).netloc for url in urls]
        self._consumer_channels: set[str] = set()
        self._partitions: dict[str, set[TopicPartition]] = {}
        self._ready = asyncio.Event()
        # subscribe/unsubscribe change the consumer's assignment; one at a time.
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._refresher: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)  # pyright: ignore
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self._servers)  # pyright: ignore
        await self._producer.start()
        await self._consumer.start()
        self._stop.clear()
        self._refresher = asyncio.create_task(self._refresh_partitions())

    async def disconnect(self) -> None:
        self._stop.set()
        refresher, self._refresher = self._refresher, None
        if refresher is not None:
            await stop_task(refresher, self._SUBSCRIBE_TIMEOUT)
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._SUBSCRIBE_TIMEOUT
        # partitions_for() waits for the topic's metadata. For a topic that
        # does not exist yet it also triggers auto-creation (if the broker
        # allows it), so a new topic is usable at once instead of after the
        # consumer's next periodic metadata refresh (5 minutes by default).
        # The metadata and the start offsets are looked up outside the lock:
        # a slow topic must not hold up subscription changes on other channels.
        try:
            partition_ids = await asyncio.wait_for(self._producer.partitions_for(channel), self._SUBSCRIBE_TIMEOUT)
        except asyncio.TimeoutError:
            raise ConnectionError(
                f"Kafka: no metadata for topic {channel!r} within {self._SUBSCRIBE_TIMEOUT:g} s "
                "(does the topic exist, or is topic auto-creation enabled?)"
            ) from None
        new = [TopicPartition(channel, partition) for partition in partition_ids]
        try:
            # Start at the current end, so messages published after
            # subscribe() returns are delivered.
            offsets = await asyncio.wait_for(self._consumer.end_offsets(new), max(deadline - loop.time(), 0.0))
            await asyncio.wait_for(self._add_channel(channel, offsets), max(deadline - loop.time(), 0.0))
        except asyncio.TimeoutError:
            raise ConnectionError(
                f"Kafka: could not start reading topic {channel!r} within {self._SUBSCRIBE_TIMEOUT:g} s"
            ) from None

    async def _add_channel(self, channel: str, offsets: dict[TopicPartition, int]) -> None:
        async with self._lock:
            self._partitions[channel] = set(offsets)
            self._consumer_channels.add(channel)
            try:
                await self._reassign(offsets)
            except BaseException:
                # Failed, timed out or cancelled before the assignment changed;
                # Broadcast will not call unsubscribe().
                self._consumer_channels.discard(channel)
                self._partitions.pop(channel, None)
                raise
            self._ready.set()

    async def unsubscribe(self, channel: str) -> None:
        # Unsubscribe only this channel (encode/broadcaster#65): upstream called
        # the consumer's unsubscribe(), which dropped every topic.
        async with self._lock:
            self._consumer_channels.discard(channel)
            self._partitions.pop(channel, None)
            if self._consumer_channels:
                await self._reassign({})
            else:
                self._ready.clear()
                self._consumer.unsubscribe()

    async def _reassign(self, offsets: dict[TopicPartition, int]) -> None:
        """Assign the partitions of every subscribed channel; must be called with `_lock` held.

        `offsets` gives the start offsets of the newly added partitions, looked
        up beforehand. assign() resets every partition's read position, so the
        positions of the partitions that stay assigned are read first and
        restored after. The assignment changes only at the end, in one step
        with no await in between, so a failure or cancellation leaves it as it
        was, and no message on a kept partition is delivered twice.
        """
        wanted = set().union(*self._partitions.values())
        kept = (wanted - offsets.keys()) & self._consumer.assignment()
        positions = {tp: await self._consumer.position(tp) for tp in kept}
        positions.update(offsets)
        if wanted:
            self._consumer.assign(list(wanted))
            for tp, offset in positions.items():
                if tp in wanted:
                    self._consumer.seek(tp, offset)
        else:
            self._consumer.unsubscribe()

    async def _refresh_partitions(self) -> None:
        """Pick up partitions added to subscribed topics.

        A manual assignment is fixed, unlike a topic subscription, so this
        checks the topics' metadata periodically.
        """
        while True:
            await wait_or_stop(self._stop, self._PARTITION_REFRESH_INTERVAL)
            if self._stop.is_set():
                return
            if not self._consumer_channels:
                continue
            try:
                await asyncio.wait_for(self._producer.client.force_metadata_update(), self._SUBSCRIBE_TIMEOUT)
            except Exception:
                logger.warning("Could not check Kafka topics for new partitions", exc_info=True)
                continue
            for channel in list(self._consumer_channels):
                try:
                    await self._add_new_partitions(channel)
                except Exception:
                    logger.warning("Could not check Kafka topic %r for new partitions", channel, exc_info=True)

    async def _add_new_partitions(self, channel: str) -> None:
        # Network lookups happen outside the lock, each within a bound; the
        # lock is held only to compare and update the assignment.
        ids = await asyncio.wait_for(self._producer.partitions_for(channel), self._SUBSCRIBE_TIMEOUT)
        current = self._partitions.get(channel)
        if current is None:
            return  # unsubscribed meanwhile
        added = [tp for tp in (TopicPartition(channel, i) for i in ids) if tp not in current]
        if not added:
            return
        # From the beginning: everything published to them is new to us.
        offsets = await asyncio.wait_for(self._consumer.beginning_offsets(added), self._SUBSCRIBE_TIMEOUT)
        async with self._lock:
            current = self._partitions.get(channel)
            if current is None:
                return
            offsets = {tp: offset for tp, offset in offsets.items() if tp not in current}
            if not offsets:
                return
            logger.info("Kafka topic %r has %d new partition(s); reading them", channel, len(offsets))
            self._partitions[channel] = current | offsets.keys()
            try:
                await asyncio.wait_for(self._reassign(offsets), self._SUBSCRIBE_TIMEOUT)
            except BaseException:
                self._partitions[channel] = current
                raise

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
