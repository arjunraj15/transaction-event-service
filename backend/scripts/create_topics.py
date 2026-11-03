"""Idempotently create the Kafka topics. Run once per environment:

uv run python scripts/create_topics.py
"""

import asyncio  # Run the async admin client.
import contextlib  # suppress() to ignore an expected exception.

from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # Kafka admin client and topic definition type.
from aiokafka.errors import TopicAlreadyExistsError  # Raised when a topic already exists.

from txn_service.config import get_settings  # Topic settings.
from txn_service.messaging.kafka import connection_kwargs  # Shared broker connection/auth options.

# Retention period (7 days) in milliseconds, as a string because Kafka configs are strings.
SEVEN_DAYS_MS = str(7 * 24 * 60 * 60 * 1000)


async def main() -> None:
    # Load configuration.
    settings = get_settings()
    # Admin client using the same connection settings as the app.
    admin = AIOKafkaAdminClient(**connection_kwargs(settings))
    # Connect to the cluster.
    await admin.start()
    try:
        # Names of topics already on the cluster.
        existing = set(await admin.list_topics())
        # With 3 replicas require 2 in sync for a write to succeed; otherwise 1 (single broker).
        min_isr = "2" if settings.kafka_topic_replication_factor >= 3 else "1"
        # Build definitions only for topics that are missing.
        wanted = [
            NewTopic(
                name=name,
                num_partitions=settings.kafka_topic_partitions,
                replication_factor=settings.kafka_topic_replication_factor,
                # Keep messages 7 days and enforce the durability setting above.
                topic_configs={"retention.ms": SEVEN_DAYS_MS, "min.insync.replicas": min_isr},
            )
            # Both the main topic and its dead-letter topic.
            for name in (settings.kafka_topic_transactions, settings.kafka_topic_dlq)
            # Skip ones that exist, which makes the script safe to re-run.
            if name not in existing
        ]
        # Nothing to create: report and stop (finally still closes the client).
        if not wanted:
            print("topics already exist")
            return
        with contextlib.suppress(TopicAlreadyExistsError):  # created concurrently by another run
            # Send the creation request to the cluster.
            await admin.create_topics(wanted)
        # Report which topics were requested.
        print("created:", ", ".join(t.name for t in wanted))
    finally:
        # Always release the connection.
        await admin.close()


# Only run when executed as a script.
if __name__ == "__main__":
    # Start the event loop and run main().
    asyncio.run(main())
