# Used for timeouts and gathering tasks.
import asyncio

# UTC-aware timestamps.
from datetime import UTC, datetime

# Type for loosely typed kwargs.
from typing import Any

# Async Kafka client classes and partition type.
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

# Helper to build a TLS context for Kafka.
from aiokafka.helpers import create_ssl_context

# Settings model.
from txn_service.config import Settings

# Event type published to Kafka.
from txn_service.messaging.schemas import TransactionEvent


# Build the connection options shared by producers and consumers.
def connection_kwargs(settings: Settings) -> dict[str, Any]:
    """Shared client config. Confluent Cloud = SASL_SSL + PLAIN with an API key/secret."""
    # Base options for any connection.
    kwargs: dict[str, Any] = {
        # Broker addresses.
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        # Plaintext or SASL_SSL.
        "security_protocol": settings.kafka_security_protocol,
    }
    # Add authentication options only for SASL_SSL.
    if settings.kafka_security_protocol == "SASL_SSL":
        # Merge the extra options.
        kwargs.update(
            # Authentication mechanism, e.g. PLAIN.
            sasl_mechanism=settings.kafka_sasl_mechanism,
            # API key as username.
            sasl_plain_username=settings.kafka_api_key,
            # API secret as password; unwrap the secret value if set.
            sasl_plain_password=(
                settings.kafka_api_secret.get_secret_value() if settings.kafka_api_secret else None
            ),
            # TLS context to encrypt the connection.
            ssl_context=create_ssl_context(),
        )
    # Return the options.
    return kwargs


# Create the Kafka producer (not started yet).
def create_producer(settings: Settings) -> AIOKafkaProducer:
    # Build the producer.
    return AIOKafkaProducer(
        # Shared connection options.
        **connection_kwargs(settings),
        acks="all",  # durable: written to all in-sync replicas before we answer 202
        enable_idempotence=True,  # broker drops producer-retry duplicates
        # Batch sends for 2 ms to improve throughput.
        linger_ms=2,
        # Fail a request if the broker is silent for 10 s.
        request_timeout_ms=10_000,
    )


# Create the Kafka consumer (not started yet).
def create_consumer(settings: Settings) -> AIOKafkaConsumer:
    """Not subscribed yet: the worker subscribes with its rebalance listener."""
    # Build the consumer.
    return AIOKafkaConsumer(
        # Shared connection options.
        **connection_kwargs(settings),
        # Consumer group id for partition sharing.
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,  # offsets are committed only after the DB write succeeds
        # With no committed offset, start at the beginning.
        auto_offset_reset="earliest",
        # Evict the consumer if no heartbeat arrives in 45 s.
        session_timeout_ms=45_000,
        # Send heartbeats every 3 s.
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,  # >> processing delay, so a waiting consumer is never evicted
        # Cap how many messages one poll returns.
        max_poll_records=200,
    )


# Writes unprocessable messages to the dead-letter topic.
class KafkaDeadLetterSink:
    """Parks messages that cannot be processed on the DLQ topic, with the reason in headers."""

    # Store the producer and target topic.
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        # Keep the producer.
        self._producer = producer
        # Keep the DLQ topic name.
        self._topic = topic

    # Send the raw message with failure details.
    async def send(self, raw: bytes, key: bytes | None, reason: str) -> None:
        # Send and wait for the broker ack.
        await self._producer.send_and_wait(
            # Destination topic.
            self._topic,
            # Original bytes unchanged.
            value=raw,
            # Original key, preserved for partitioning.
            key=key,
            # Metadata headers.
            headers=[
                # Why it failed.
                ("error_reason", reason.encode()),
                # When it failed, in ISO-8601.
                ("failed_at", datetime.now(UTC).isoformat().encode()),
            ],
        )


# Publishes transaction events to the main topic.
class TransactionPublisher:
    # Store the producer and settings.
    def __init__(self, producer: AIOKafkaProducer, settings: Settings) -> None:
        # Keep the producer.
        self._producer = producer
        # Keep the settings.
        self._settings = settings

    # Send one event to Kafka.
    async def publish(self, event: TransactionEvent) -> None:
        """Wait for the broker ack, bounded so the webhook stays inside its latency budget.

        On timeout the caller returns 503 and the processor retries; if the original send did land,
        the retry becomes a duplicate event, which the consumer's idempotent insert absorbs.
        """
        # Abort if the broker takes longer than the configured limit.
        async with asyncio.timeout(self._settings.produce_timeout_seconds):
            # Send and wait for the broker ack.
            await self._producer.send_and_wait(
                # Destination topic.
                self._settings.kafka_topic_transactions,
                # Event serialized to JSON bytes.
                value=event.model_dump_json().encode(),
                key=event.transaction_id.encode(),  # same id -> same partition -> ordered handling
            )

    # Pre-connect to the brokers before serving traffic.
    async def warm_up(self) -> None:
        """Pay the cold-start cost before taking traffic, not inside the first webhooks' latency budget.

        The producer otherwise fetches topic metadata and opens a TLS+SASL connection to each partition
        leader lazily on the first send, which alone can exceed the 500 ms SLA. Best effort: failures
        are logged by the caller and the first sends simply warm up instead.
        """
        # The topic to warm up.
        topic = self._settings.kafka_topic_transactions
        # Bound the whole warm-up to 15 s.
        async with asyncio.timeout(15):
            # Fetch topic metadata, which lists its partitions.
            partitions = await self._producer.partitions_for(topic)
            # Access the producer's low-level client.
            client = self._producer.client
            # Only this topic's partition leaders: a shared cluster can have many more brokers.
            # Find the leader broker for each partition.
            leaders = {client.cluster.leader_for_partition(TopicPartition(topic, p)) for p in partitions}
            # Open connections to all leaders concurrently.
            await asyncio.gather(*(client.ready(node_id) for node_id in leaders if node_id is not None))

    # Readiness check for the Kafka connection.
    async def is_ready(self) -> bool:
        # Any error means not ready.
        try:
            # Fail fast after 2 s.
            async with asyncio.timeout(2):
                # Fetch partition metadata for the topic.
                partitions = await self._producer.partitions_for(self._settings.kafka_topic_transactions)
        # Treat any error as not ready.
        except Exception:
            # Report not ready.
            return False
        # Ready only if the topic has partitions.
        return bool(partitions)
