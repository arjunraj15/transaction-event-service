import json
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from tests.fakes import FakePublisher, FakeRepository
from txn_service.api.app import create_app
from txn_service.api.security import compute_signature
from txn_service.config import Settings
from txn_service.messaging.schemas import TransactionEvent, WebhookPayload

URL = "/v1/webhooks/transactions"
PAYLOAD = {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


def make_client(publisher: FakePublisher, repo: FakeRepository, **settings: object) -> Iterator[TestClient]:
    app = create_app(Settings(**settings), publisher=publisher, reader=repo)  # type: ignore[arg-type]
    with TestClient(app) as client:
        yield client


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def repo() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def client(publisher: FakePublisher, repo: FakeRepository) -> Iterator[TestClient]:
    yield from make_client(publisher, repo)


def test_health_matches_contract(client: TestClient) -> None:
    body = client.get("/").json()
    assert body["status"] == "HEALTHY"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", body["current_time"])


def test_webhook_is_acknowledged_with_202_and_published(client: TestClient, publisher: FakePublisher) -> None:
    start = time.perf_counter()
    response = client.post(URL, json=PAYLOAD)
    assert response.status_code == 202
    assert time.perf_counter() - start < 0.5
    assert response.json() == {"status": "accepted", "transaction_id": "txn_abc123def456"}
    [event] = publisher.events
    assert event.amount == Decimal(1500)
    assert event.received_at.tzinfo is not None


def test_invalid_payload_returns_422_and_is_not_published(
    client: TestClient, publisher: FakePublisher
) -> None:
    assert client.post(URL, json={**PAYLOAD, "amount": -1}).status_code == 422
    assert (
        client.post(URL, content=b"not json", headers={"content-type": "application/json"}).status_code == 422
    )
    assert publisher.events == []


def test_broker_failure_returns_503_so_sender_retries(repo: FakeRepository) -> None:
    for client in make_client(FakePublisher(error=TimeoutError()), repo):
        response = client.post(URL, json=PAYLOAD)
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"


def test_signature_is_enforced_when_secret_configured(publisher: FakePublisher, repo: FakeRepository) -> None:
    for client in make_client(publisher, repo, webhook_secret=SecretStr("s3cret")):
        body = json.dumps(PAYLOAD).encode()
        headers = {"content-type": "application/json"}
        assert client.post(URL, content=body, headers=headers).status_code == 401
        bad = {**headers, "X-Razorpay-Signature": "deadbeef"}
        assert client.post(URL, content=body, headers=bad).status_code == 401
        good = {**headers, "X-Razorpay-Signature": compute_signature("s3cret", body)}
        assert client.post(URL, content=body, headers=good).status_code == 202
        assert len(publisher.events) == 1


def test_blank_webhook_secret_disables_verification(publisher: FakePublisher, repo: FakeRepository) -> None:
    # Render sets an optional variable left blank to "": that must mean "disabled", not "secret is empty".
    assert Settings(webhook_secret="  ").webhook_secret is None  # type: ignore[arg-type]
    for client in make_client(publisher, repo, webhook_secret=""):
        assert client.post(URL, json=PAYLOAD).status_code == 202


async def _seed(repo: FakeRepository) -> None:
    payload = WebhookPayload.model_validate({**PAYLOAD, "amount": "150.50", "currency": "USD"})
    await repo.insert_if_absent(
        TransactionEvent.from_webhook(payload, datetime(2024, 1, 15, 10, 30, tzinfo=UTC))
    )


async def test_get_transaction_returns_array_contract(client: TestClient, repo: FakeRepository) -> None:
    await _seed(repo)
    body = client.get("/v1/transactions/txn_abc123def456").json()
    assert body == [
        {
            "transaction_id": "txn_abc123def456",
            "source_account": "acc_user_789",
            "destination_account": "acc_merchant_456",
            "amount": 150.5,
            "currency": "USD",
            "status": "PROCESSING",
            "created_at": "2024-01-15T10:30:00Z",
            "processed_at": None,
        }
    ]


def test_get_unknown_transaction_returns_404(client: TestClient) -> None:
    assert client.get("/v1/transactions/nope").status_code == 404


def test_readiness_reflects_dependencies(client: TestClient, publisher: FakePublisher) -> None:
    assert client.get("/health/ready").status_code == 200
    publisher.ready = False
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["checks"] == {"database": True, "kafka": False}


def test_metrics_endpoint_exposes_webhook_counter(client: TestClient) -> None:
    client.post(URL, json=PAYLOAD)
    assert 'webhooks_received_total{outcome="accepted"}' in client.get("/metrics").text
