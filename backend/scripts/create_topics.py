"""Idempotently create the Kafka topics. Run once per environment:

uv run python scripts/create_topics.py
"""

import asyncio
import contextlib

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from txn_service.config import get_settings
from txn_service.messaging.kafka import connection_kwargs

SEVEN_DAYS_MS = str(7 * 24 * 60 * 60 * 1000)


async def main() -> None:
    settings = get_settings()
    admin = AIOKafkaAdminClient(**connection_kwargs(settings))
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        min_isr = "2" if settings.kafka_topic_replication_factor >= 3 else "1"
        wanted = [
            NewTopic(
                name=name,
                num_partitions=settings.kafka_topic_partitions,
                replication_factor=settings.kafka_topic_replication_factor,
                topic_configs={"retention.ms": SEVEN_DAYS_MS, "min.insync.replicas": min_isr},
            )
            for name in (settings.kafka_topic_transactions, settings.kafka_topic_dlq)
            if name not in existing
        ]
        if not wanted:
            print("topics already exist")
            return
        with contextlib.suppress(TopicAlreadyExistsError):  # created concurrently by another run
            await admin.create_topics(wanted)
        print("created:", ", ".join(t.name for t in wanted))
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
