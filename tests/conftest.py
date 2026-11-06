from __future__ import annotations

import asyncio

import pytest

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPICS = ["chatroom"]

_kafka_topics_ready = False


async def _create_kafka_topics() -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    await admin.start()
    try:
        topics = [NewTopic(name, num_partitions=1, replication_factor=1) for name in KAFKA_TOPICS]
        try:
            await admin.create_topics(topics)
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()


def pytest_runtest_setup(item: pytest.Item) -> None:
    # On a fresh broker the test topics do not exist yet. Publishing auto-creates
    # them, but an already-subscribed aiokafka consumer only sees new topics on its
    # next metadata refresh (metadata_max_age_ms, default 5 minutes), so every
    # Kafka test run on a fresh broker (e.g. in CI) would take 5 minutes.
    global _kafka_topics_ready
    if _kafka_topics_ready or "kafka" not in item.name:
        return
    asyncio.run(_create_kafka_topics())
    _kafka_topics_ready = True
