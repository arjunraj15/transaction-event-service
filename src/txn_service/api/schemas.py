from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, PlainSerializer

# Stored and computed as Decimal; rendered as a JSON number to match the published API contract.
JsonNumber = Annotated[Decimal, PlainSerializer(float, return_type=float, when_used="json")]


class HealthResponse(BaseModel):
    status: Literal["HEALTHY"] = "HEALTHY"
    current_time: str


class ReadinessResponse(BaseModel):
    status: Literal["READY", "NOT_READY"]
    checks: dict[str, bool]


class WebhookAccepted(BaseModel):
    status: Literal["accepted"] = "accepted"
    transaction_id: str


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: JsonNumber
    currency: str
    status: str
    created_at: datetime
    processed_at: datetime | None
