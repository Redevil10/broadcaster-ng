from __future__ import annotations

import asyncio
import logging
import time
import typing
import uuid

import pytest

from broadcaster import Broadcast
from broadcaster.backends.kafka import KafkaBackend

KAFKA_URL = "kafka://localhost:9092"


def new_topic() -> str:
    return f"test_{uuid.uuid4().hex[:12]}"


@pytest.mark.asyncio
async def test_subscribe_to_new_topic_is_quick() -> None:
    # A topic that does not exist yet used to take until the consumer's next
    # metadata refresh (5 minutes by default) before subscribe() returned.
    topic = new_topic()
    async with Broadcast(KAFKA_URL) as broadcast:
        started = time.monotonic()
        async with broadcast.subscribe(topic) as subscriber:
            assert time.monotonic() - started < 5
            await broadcast.publish(topic, "hello")
            assert (await asyncio.wait_for(subscriber.get(), 5)).message == "hello"


@pytest.mark.asyncio
async def test_unsubscribing_one_channel_keeps_the_others() -> None:
    # encode/broadcaster#65: unsubscribing any channel unsubscribed all of them.
    # Also, changing the subscription must not lose messages on the others.
    kept, churned = new_topic(), new_topic()
    count = 30
    async with Broadcast(KAFKA_URL) as broadcast:
        async with broadcast.subscribe(kept) as subscriber:

            async def churn() -> None:
                for _ in range(5):
                    async with broadcast.subscribe(churned):
                        await asyncio.sleep(0.01)

            async def publish() -> None:
                for n in range(count):
                    await broadcast.publish(kept, str(n))
                    await asyncio.sleep(0.005)

            await asyncio.gather(churn(), publish())
            received = [(await asyncio.wait_for(subscriber.get(), 5)).message for _ in range(count)]
    assert received == [str(n) for n in range(count)]


@pytest.mark.asyncio
async def test_resubscribe_after_last_unsubscribe(caplog: pytest.LogCaptureFixture) -> None:
    topic = new_topic()
    with caplog.at_level(logging.WARNING, logger="broadcaster"):
        async with Broadcast(KAFKA_URL) as broadcast:
            async with broadcast.subscribe(topic) as subscriber:
                await broadcast.publish(topic, "first")
                assert (await asyncio.wait_for(subscriber.get(), 5)).message == "first"
            await asyncio.sleep(0.2)
            async with broadcast.subscribe(topic) as subscriber:
                await broadcast.publish(topic, "second")
                assert (await asyncio.wait_for(subscriber.get(), 5)).message == "second"
    # aiokafka itself logs "Topic ... not found" while a new topic is created;
    # only our own logger must stay quiet.
    assert [r for r in caplog.records if r.name.startswith("broadcaster")] == []


@pytest.mark.asyncio
async def test_subscribe_times_out_when_topic_metadata_never_arrives(monkeypatch: pytest.MonkeyPatch) -> None:
    # E.g. the topic does not exist and the broker does not auto-create topics.
    monkeypatch.setattr(KafkaBackend, "_SUBSCRIBE_TIMEOUT", 0.2)
    backend = KafkaBackend(KAFKA_URL)
    async with Broadcast(backend=backend) as broadcast:

        async def never(_topic: str) -> typing.Any:
            await asyncio.sleep(60)

        monkeypatch.setattr(backend._producer, "partitions_for", never)
        with pytest.raises(ConnectionError, match="no metadata for topic"):
            async with broadcast.subscribe(new_topic()):
                pass  # pragma: no cover
        assert backend._consumer_channels == set()


@pytest.mark.asyncio
async def test_slow_topic_metadata_does_not_hold_up_other_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    backend = KafkaBackend(KAFKA_URL)
    slow, other = new_topic(), new_topic()
    async with Broadcast(backend=backend) as broadcast:
        original = backend._producer.partitions_for

        async def partitions_for(topic: str) -> typing.Any:
            if topic == slow:
                await asyncio.sleep(3)
            return await original(topic)

        monkeypatch.setattr(backend._producer, "partitions_for", partitions_for)

        async def join_slow() -> None:
            async with broadcast.subscribe(slow):
                pass

        slow_task = asyncio.create_task(join_slow())
        await asyncio.sleep(0.1)
        started = time.monotonic()
        async with broadcast.subscribe(other) as subscriber:
            assert time.monotonic() - started < 2
            await broadcast.publish(other, "hello")
            assert (await asyncio.wait_for(subscriber.get(), 5)).message == "hello"
        slow_task.cancel()
        await asyncio.gather(slow_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stuck_offset_lookup_times_out_and_restores_the_assignment(monkeypatch: pytest.MonkeyPatch) -> None:
    # E.g. metadata is cached but the partition leader is unavailable.
    monkeypatch.setattr(KafkaBackend, "_SUBSCRIBE_TIMEOUT", 2.0)
    backend = KafkaBackend(KAFKA_URL)
    kept, stuck = new_topic(), new_topic()
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe(kept) as subscriber:
            assignment = backend._consumer.assignment()
            original = backend._consumer.seek_to_end

            async def seek_to_end(*partitions: typing.Any) -> typing.Any:
                if any(tp.topic == stuck for tp in partitions):
                    await asyncio.sleep(60)
                return await original(*partitions)

            monkeypatch.setattr(backend._consumer, "seek_to_end", seek_to_end)
            with pytest.raises(ConnectionError, match="could not start reading topic"):
                async with broadcast.subscribe(stuck):
                    pass  # pragma: no cover
            assert backend._consumer.assignment() == assignment
            assert backend._consumer_channels == {kept}
            await broadcast.publish(kept, "still here")
            assert (await asyncio.wait_for(subscriber.get(), 5)).message == "still here"


@pytest.mark.asyncio
async def test_partitions_added_to_a_subscribed_topic_are_read(monkeypatch: pytest.MonkeyPatch) -> None:
    from aiokafka import AIOKafkaProducer
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic

    monkeypatch.setattr(KafkaBackend, "_PARTITION_REFRESH_INTERVAL", 0.2)
    topic = new_topic()
    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await admin.start()
    await producer.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        async with Broadcast(KAFKA_URL) as broadcast:
            async with broadcast.subscribe(topic) as subscriber:
                await admin.create_partitions({topic: NewPartitions(total_count=3)})
                await producer.client.force_metadata_update()
                while len(await producer.partitions_for(topic)) < 3:
                    await asyncio.sleep(0.1)
                    await producer.client.force_metadata_update()
                await producer.send_and_wait(topic, b"on a new partition", partition=2)
                assert (await asyncio.wait_for(subscriber.get(), 10)).message == "on a new partition"
    finally:
        await producer.stop()
        await admin.close()
