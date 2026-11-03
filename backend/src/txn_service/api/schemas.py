# Timestamp type for response fields.
from datetime import datetime

# Exact decimal type for money.
from decimal import Decimal

# Annotated adds serializer metadata; Literal restricts a field to fixed values.
from typing import Annotated, Literal

# Pydantic base model, config and custom serializer helpers.
from pydantic import BaseModel, ConfigDict, PlainSerializer

# Stored and computed as Decimal; rendered as a JSON number to match the published API contract.
JsonNumber = Annotated[Decimal, PlainSerializer(float, return_type=float, when_used="json")]


# Response body for the liveness endpoint.
class HealthResponse(BaseModel):
    # Always the constant "HEALTHY".
    status: Literal["HEALTHY"] = "HEALTHY"
    # Current UTC time as a formatted string.
    current_time: str


# Response body for the readiness endpoint.
class ReadinessResponse(BaseModel):
    # Overall verdict.
    status: Literal["READY", "NOT_READY"]
    # Per-dependency result, e.g. database and kafka.
    checks: dict[str, bool]


# Non-secret runtime info for the dashboard.
class ServiceInfo(BaseModel):
    # Deployment environment name.
    environment: str
    # Simulated processing delay, used for the dashboard countdown.
    processing_delay_seconds: float
    # Whether webhooks must be signed.
    signature_required: bool
    # Whether admin endpoints need a key.
    admin_key_required: bool


# Response body for an accepted webhook.
class WebhookAccepted(BaseModel):
    # Always the constant "accepted".
    status: Literal["accepted"] = "accepted"
    # Id of the transaction that was queued.
    transaction_id: str


# Public representation of a transaction.
class TransactionOut(BaseModel):
    # Allow building this model directly from ORM objects.
    model_config = ConfigDict(from_attributes=True)

    # Unique transaction id.
    transaction_id: str
    # Account the money leaves.
    source_account: str
    # Account the money goes to.
    destination_account: str
    # Amount, serialized as a JSON number.
    amount: JsonNumber
    # Three-letter currency code.
    currency: str
    # PROCESSING, PROCESSED or FAILED.
    status: str
    # When the webhook was received.
    created_at: datetime
    # When processing finished; None while still pending.
    processed_at: datetime | None
