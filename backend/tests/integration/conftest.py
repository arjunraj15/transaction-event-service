"""Real Postgres + Kafka via Testcontainers. Skipped automatically when Docker is unavailable."""

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient

from txn_service.api.app import create_app
from txn_service.config import Settings, get_settings
from txn_service.db.engine import create_engine, create_session_factory
from txn_service.db.repository import TransactionRepository
from txn_service.messaging.kafka import KafkaDeadLetterSink, create_consumer, create_producer
from txn_service.worker.consumer import ConsumerRunner
from txn_service.worker.processor import TransactionProcessor

pytestmark = pytest.mark.integration
PROCESSING_DELAY = 2.0


@dataclass
class Infra:
    database_url: str
    bootstrap_servers: str


@pytest.fixture(scope="session")
def infra() -> Iterator[Infra]:
    try:
        from testcontainers.community.kafka import KafkaContainer
        from testcontainers.community.postgres import PostgresContainer

        postgres = PostgresContainer("postgres:16-alpine", driver=None)
        kafka = KafkaContainer("confluentinc/cp-kafka:7.6.1").with_kraft()
        postgres.start()
        kafka.start()
    except Exception as exc:
        pytest.skip(f"Docker not available for integration tests: {exc}")

    database_url = postgres.get_connection_url()
    os.environ["DATABASE_URL"] = database_url
    get_settings.cache_clear()
    command.upgrade(Config("alembic.ini"), "head")  # exercise the real migration
    try:
        yield Infra(database_url=database_url, bootstrap_servers=kafka.get_bootstrap_server())
    finally:
        kafka.stop()
        postgres.stop()


@pytest.fixture
async def settings(infra: Infra) -> Settings:
    """Fresh topics + consumer group per test so tests cannot see each other's messages."""
    suffix = uuid.uuid4().hex[:8]
    settings = Settings(
        database_url=infra.database_url,
        kafka_bootstrap_servers=infra.bootstrap_servers,
        kafka_topic_transactions=f"transactions.received.{suffix}",
        kafka_topic_dlq=f"transactions.dlq.{suffix}",
        kafka_consumer_group=f"processor-{suffix}",
        kafka_topic_partitions=3,
        processing_delay_seconds=PROCESSING_DELAY,
        processing_retry_backoff_seconds=0.1,
        produce_timeout_seconds=5.0,  # first send to a fresh topic waits for metadata
    )
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=infra.bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(settings.kafka_topic_transactions, 3, 1),
                NewTopic(settings.kafka_topic_dlq, 1, 1),
            ]
        )
    finally:
        await admin.close()
    return settings


@pytest.fixture
async def api(settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    transport = ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=transport, base_url="http://test") as client,
    ):
        yield client


@pytest.fixture
async def repository(settings: Settings) -> AsyncIterator[TransactionRepository]:
    engine = create_engine(settings)
    yield TransactionRepository(create_session_factory(engine))
    await engine.dispose()


WorkerHandle = Callable[[], Awaitable[None]]  # awaiting it stops the worker (simulated crash)


@pytest.fixture
async def start_worker(
    settings: Settings, repository: TransactionRepository
) -> AsyncIterator[Callable[[], Awaitable[WorkerHandle]]]:
    running: list[WorkerHandle] = []

    async def start() -> WorkerHandle:
        producer, consumer = create_producer(settings), create_consumer(settings)
        await producer.start()
        await consumer.start()
        processor = TransactionProcessor(
            repository, KafkaDeadLetterSink(producer, settings.kafka_topic_dlq), settings
        )
        stop = asyncio.Event()
        runner = ConsumerRunner(consumer, processor, settings.kafka_topic_transactions)
        task = asyncio.create_task(runner.run(stop))

        async def halt() -> None:
            if task.done():
                return
            task.cancel()  # hard stop: in-flight work is abandoned without committing
            await asyncio.gather(task, return_exceptions=True)
            await consumer.stop()
            await producer.stop()

        running.append(halt)
        return halt

    yield start
    for halt in running:
        await halt()
