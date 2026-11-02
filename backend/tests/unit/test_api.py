import json
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from tests.fakes import FakeEventSource, FakePublisher, FakeRepository
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


def make_client(
    publisher: FakePublisher,
    repo: FakeRepository,
    events: FakeEventSource | None = None,
    **settings: object,
) -> Iterator[TestClient]:
    app = create_app(
        Settings(**settings),  # type: ignore[arg-type]
        publisher=publisher,
        reader=repo,
        event_source=events or FakeEventSource(),
    )
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


async def test_list_transactions_newest_first_with_status_filter(
    client: TestClient, repo: FakeRepository
) -> None:
    for i, minute in enumerate([1, 3, 2]):
        payload = WebhookPayload.model_validate({**PAYLOAD, "transaction_id": f"txn_{i}"})
        await repo.insert_if_absent(
            TransactionEvent.from_webhook(payload, datetime(2024, 1, 15, 10, minute, tzinfo=UTC))
        )
    await repo.mark_processed("txn_2", datetime(2024, 1, 15, 10, 4, tzinfo=UTC))

    ids = [t["transaction_id"] for t in client.get("/v1/transactions").json()]
    assert ids == ["txn_1", "txn_2", "txn_0"]
    assert [t["transaction_id"] for t in client.get("/v1/transactions?limit=1").json()] == ["txn_1"]
    processed = client.get("/v1/transactions?status=PROCESSED").json()
    assert [t["transaction_id"] for t in processed] == ["txn_2"]
    assert client.get("/v1/transactions?status=BOGUS").status_code == 422
    assert client.get("/v1/transactions?limit=0").status_code == 422


def test_meta_exposes_processing_delay(client: TestClient) -> None:
    body = client.get("/v1/meta").json()
    assert body["processing_delay_seconds"] == 30.0
    assert body["signature_required"] is False


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


def test_event_stream_sends_hello_then_events(publisher: FakePublisher, repo: FakeRepository) -> None:
    events = FakeEventSource(payloads=['{"type":"processed","transaction_id":"txn_1"}'])
    for client in make_client(publisher, repo, events):
        with client.stream("GET", "/v1/events/stream") as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            body = "".join(response.iter_text())
    assert "retry: 3000" in body
    assert "event: hello" in body and '"connected": true' in body
    assert 'data: {"type":"processed","transaction_id":"txn_1"}' in body
    assert events.subscribers == []  # unsubscribed when the stream ended


def test_admin_endpoints_require_the_admin_key_when_configured(
    publisher: FakePublisher, repo: FakeRepository
) -> None:
    events = FakeEventSource()
    for client in make_client(publisher, repo, events, admin_api_key=SecretStr("adm1n")):
        for path in ("/v1/transactions", "/v1/events/stream"):
            assert client.get(path).status_code == 401
            assert client.get(path, headers={"X-Admin-Key": "wrong"}).status_code == 401
        assert client.get("/v1/transactions", headers={"X-Admin-Key": "adm1n"}).status_code == 200
        with client.stream("GET", "/v1/events/stream", headers={"X-Admin-Key": "adm1n"}) as response:
            assert response.status_code == 200
        assert client.get("/v1/meta").json()["admin_key_required"] is True


def test_graded_endpoints_stay_open_when_admin_key_is_set(
    publisher: FakePublisher, repo: FakeRepository
) -> None:
    for client in make_client(publisher, repo, admin_api_key=SecretStr("adm1n")):
        assert client.get("/").json()["status"] == "HEALTHY"
        assert client.post(URL, json=PAYLOAD).status_code == 202
        assert client.get("/v1/transactions/txn_abc123def456").status_code == 404  # open, just unknown


def test_cors_allows_only_the_configured_dashboard_origin(
    publisher: FakePublisher, repo: FakeRepository
) -> None:
    preflight = {"Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "X-Admin-Key"}
    # A bare host (as Render's fromService provides) is normalized to https://host
    for client in make_client(publisher, repo, cors_allowed_origins="dashboard.example.com"):
        ok = client.options(
            "/v1/transactions", headers={"Origin": "https://dashboard.example.com", **preflight}
        )
        assert ok.headers["access-control-allow-origin"] == "https://dashboard.example.com"
        assert "x-admin-key" in ok.headers["access-control-allow-headers"].lower()
        evil = client.options("/v1/transactions", headers={"Origin": "https://evil.example", **preflight})
        assert "access-control-allow-origin" not in evil.headers


def test_cors_is_off_when_no_origin_is_configured(publisher: FakePublisher, repo: FakeRepository) -> None:
    for client in make_client(publisher, repo):
        response = client.get("/", headers={"Origin": "https://dashboard.example.com"})
        assert "access-control-allow-origin" not in response.headers
