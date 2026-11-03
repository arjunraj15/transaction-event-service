# Timestamp type.
from datetime import datetime

# Exact decimal type for money.
from decimal import Decimal

# Annotated attaches constraints; Literal fixes allowed values.
from typing import Annotated, Literal

# Pydantic validation building blocks.
from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

# Safe id: trimmed, 1-128 chars, limited character set.
Identifier = Annotated[
    # Base type is str.
    str,
    # Trim whitespace, enforce length and allowed characters.
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_\-:.]+$"),
]
# ISO-4217 alphabetic code, normalized to upper case.
Currency = Annotated[
    # Base type is str.
    str,
    # Trim and require exactly three letters.
    StringConstraints(strip_whitespace=True, min_length=3, max_length=3, pattern=r"^[A-Za-z]{3}$"),
    # Convert to upper case after validation.
    AfterValidator(str.upper),
]
# Money is never a float: validated and stored as an exact Decimal.
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=4)]


# Incoming webhook body.
class WebhookPayload(BaseModel):
    """Body of POST /v1/webhooks/transactions. Unknown fields from the processor are ignored."""

    # Ignore any additional fields the processor sends.
    model_config = ConfigDict(extra="ignore")

    # Unique id of the transaction.
    transaction_id: Identifier
    # Sender account.
    source_account: Identifier
    # Receiver account.
    destination_account: Identifier
    # Validated amount.
    amount: Amount
    # Validated currency code.
    currency: Currency


# Event published to Kafka, extending the webhook payload.
class TransactionEvent(WebhookPayload):
    """Message on the `transactions.received` topic. Decimal is serialized as a string (lossless)."""

    # Schema version, so consumers can handle future changes.
    schema_version: Literal[1] = 1
    # Server-side arrival time; must include a timezone.
    received_at: AwareDatetime

    # Alternative constructor from a payload.
    @classmethod
    # Takes the payload and the arrival time.
    def from_webhook(cls, payload: WebhookPayload, received_at: datetime) -> "TransactionEvent":
        # Copy the payload fields and add the arrival time.
        return cls(**payload.model_dump(), received_at=received_at)
