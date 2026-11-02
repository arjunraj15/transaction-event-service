"""The success criteria from the brief, exercised against real Postgres and Kafka.

PROCESSING_DELAY_SECONDS is 2s here (30s in production) to keep the suite fast.
"""

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from httpx import AsyncClient
from prometheus_client import REGISTRY

from tests.integration.conftest import PROCESSING_DELAY, WorkerHandle
from txn_service.config import Settings
from txn_service.db.repository import TransactionRepository

pytestmark = pytest.mark.integration
StartWorker = Callable[[], Awaitable[WorkerHandle]]


def payload(txn_id: str) -> dict[str, Any]:
    return {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


async def wait_for_status(api: AsyncClient, txn_id: str, status: str, timeout: float = 60) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        while True:
            response = await api.get(f"/v1/transactions/{txn_id}")
            if response.status_code == 200 and response.json()[0]["status"] == status:
                body: dict[str, Any] = response.json()[0]
                return body
            await asyncio.sleep(0.2)


def processed_total() -> float:
    return REGISTRY.get_sample_value("transactions_processed_total") or 0.0


async def test_single_transaction_processed_after_delay(api: AsyncClient, start_worker: StartWorker) -> None:
    await start_worker()
    txn_id = f"txn_{uuid.uuid4().hex}"

    start = time.perf_counter()
    response = await api.post("/v1/webhooks/transactions", json=payload(txn_id))
    assert response.status_code == 202
    assert time.perf_counter() - start < 0.5

    await wait_for_status(api, txn_id, "PROCESSING", timeout=30)
    body = await wait_for_status(api, txn_id, "PROCESSED")
    assert body["processed_at"] is not None
    assert time.perf_counter() - start >= PROCESSING_DELAY


async def test_duplicate_webhooks_processed_exactly_once(
    api: AsyncClient, start_worker: StartWorker, repository: TransactionRepository
) -> None:
    await start_worker()
    txn_id = f"txn_{uuid.uuid4().hex}"
    before = processed_total()

    responses = await asyncio.gather(
        *(api.post("/v1/webhooks/transactions", json=payload(txn_id)) for _ in range(10))
    )
    assert {r.status_code for r in responses} == {202}

    await wait_for_status(api, txn_id, "PROCESSED")
    await asyncio.sleep(PROCESSING_DELAY + 1)  # let every duplicate drain through the consumer
    assert processed_total() - before == 1
    row = await repository.get(txn_id)
    assert row is not None and row.attempts == 1


async def test_worker_crash_mid_delay_does_not_lose_transaction(
    api: AsyncClient, start_worker: StartWorker, repository: TransactionRepository
) -> None:
    halt = await start_worker()
    txn_id = f"txn_{uuid.uuid4().hex}"
    assert (await api.post("/v1/webhooks/transactions", json=payload(txn_id))).status_code == 202

    await wait_for_status(api, txn_id, "PROCESSING", timeout=30)
    await halt()  # crash while waiting out the delay: offset was never committed

    await start_worker()  # replacement instance in the same consumer group
    await wait_for_status(api, txn_id, "PROCESSED")
    row = await repository.get(txn_id)
    assert row is not None and row.attempts == 1


async def test_poison_message_goes_to_dlq(settings: Settings, start_worker: StartWorker) -> None:
    await start_worker()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(settings.kafka_topic_transactions, b"{garbage", key=b"bad")
    finally:
        await producer.stop()

    dlq = AIOKafkaConsumer(
        settings.kafka_topic_dlq,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=f"dlq-reader-{uuid.uuid4().hex[:6]}",
    )
    await dlq.start()
    try:
        message = await asyncio.wait_for(dlq.getone(), timeout=60)
    finally:
        await dlq.stop()
    assert message.value == b"{garbage"
    headers = dict(message.headers)
    assert headers["error_reason"].startswith(b"invalid_message")
