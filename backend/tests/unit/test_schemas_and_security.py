from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from txn_service.api.security import compute_signature, verify_signature
from txn_service.db.engine import normalize_database_url
from txn_service.messaging.schemas import TransactionEvent, WebhookPayload

VALID = {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


def test_payload_normalizes_currency_and_keeps_decimal_exact() -> None:
    payload = WebhookPayload.model_validate({**VALID, "amount": 150.50, "currency": "usd"})
    assert payload.currency == "USD"
    assert payload.amount == Decimal("150.5")


def test_payload_ignores_unknown_processor_fields() -> None:
    assert WebhookPayload.model_validate({**VALID, "notes": "x"}).transaction_id == "txn_abc123def456"


@pytest.mark.parametrize(
    "override",
    [
        {"amount": 0},
        {"amount": -5},
        {"amount": "1.23456"},  # more than 4 decimal places
        {"currency": "RUPEE"},
        {"transaction_id": ""},
        {"transaction_id": "bad id with spaces"},
    ],
)
def test_payload_rejects_invalid_values(override: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        WebhookPayload.model_validate({**VALID, **override})


def test_event_round_trips_losslessly() -> None:
    event = TransactionEvent.from_webhook(
        WebhookPayload.model_validate({**VALID, "amount": "0.1"}), datetime(2024, 1, 15, tzinfo=UTC)
    )
    raw = event.model_dump_json()
    assert '"amount":"0.1"' in raw  # string on the wire, never a float
    assert TransactionEvent.model_validate_json(raw) == event


def test_signature_verification() -> None:
    body = b'{"a":1}'
    sig = compute_signature("secret", body)
    assert verify_signature("secret", body, sig)
    assert not verify_signature("secret", body + b" ", sig)
    assert not verify_signature("other", body, sig)
    assert not verify_signature("secret", body, None)


@pytest.mark.parametrize(
    ("raw", "expected_url", "ssl"),
    [
        (
            "postgres://u:p@host/db?sslmode=require&channel_binding=require",
            "postgresql+asyncpg://u:p@host/db",
            True,
        ),
        ("postgresql://u:p@host:5432/db", "postgresql+asyncpg://u:p@host:5432/db", False),
        ("postgresql+asyncpg://u:p@host/db", "postgresql+asyncpg://u:p@host/db", False),
    ],
)
def test_database_url_normalization(raw: str, expected_url: str, ssl: bool) -> None:
    url, connect_args = normalize_database_url(raw)
    assert url == expected_url
    assert ("ssl" in connect_args) is ssl
