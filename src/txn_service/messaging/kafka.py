import asyncio
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

from txn_service.config import Settings
from txn_service.messaging.schemas import TransactionEvent


def connection_kwargs(settings: Settings) -> dict[str, Any]:
    """Shared client config. Confluent Cloud = SASL_SSL + PLAIN with an API key/secret."""
    kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "security_protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_security_protocol == "SASL_SSL":
        kwargs.update(
            sasl_mechanism=settings.kafka_sasl_mechanism,
            sasl_plain_username=settings.kafka_api_key,
            sasl_plain_password=(
                settings.kafka_api_secret.get_secret_value() if settings.kafka_api_secret else None
            ),
            ssl_context=create_ssl_context(),
        )
    return kwargs


def create_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        **connection_kwargs(settings),
        acks="all",  # durable: written to all in-sync replicas before we answer 202
        enable_idempotence=True,  # broker drops producer-retry duplicates
        linger_ms=2,
        request_timeout_ms=10_000,
    )


def create_consumer(settings: Settings) -> AIOKafkaConsumer:
    """Not subscribed yet: the worker subscribes with its rebalance listener."""
    return AIOKafkaConsumer(
        **connection_kwargs(settings),
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,  # offsets are committed only after the DB write succeeds
        auto_offset_reset="earliest",
        session_timeout_ms=45_000,
        heartbeat_interval_ms=3_000,
        max_poll_interval_ms=300_000,  # >> processing delay, so a waiting consumer is never evicted
        max_poll_records=200,
    )


class KafkaDeadLetterSink:
    """Parks messages that cannot be processed on the DLQ topic, with the reason in headers."""

    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send(self, raw: bytes, key: bytes | None, reason: str) -> None:
        await self._producer.send_and_wait(
            self._topic,
            value=raw,
            key=key,
            headers=[
                ("error_reason", reason.encode()),
                ("failed_at", datetime.now(UTC).isoformat().encode()),
            ],
        )


class TransactionPublisher:
    def __init__(self, producer: AIOKafkaProducer, settings: Settings) -> None:
        self._producer = producer
        self._settings = settings

    async def publish(self, event: TransactionEvent) -> None:
        """Wait for the broker ack, bounded so the webhook stays inside its latency budget.

        On timeout the caller returns 503 and the processor retries; if the original send did land,
        the retry becomes a duplicate event, which the consumer's idempotent insert absorbs.
        """
        async with asyncio.timeout(self._settings.produce_timeout_seconds):
            await self._producer.send_and_wait(
                self._settings.kafka_topic_transactions,
                value=event.model_dump_json().encode(),
                key=event.transaction_id.encode(),  # same id -> same partition -> ordered handling
            )

    async def is_ready(self) -> bool:
        try:
            async with asyncio.timeout(2):
                partitions = await self._producer.partitions_for(self._settings.kafka_topic_transactions)
        except Exception:
            return False
        return bool(partitions)
