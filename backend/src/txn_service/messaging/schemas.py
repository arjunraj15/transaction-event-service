from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

Identifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_\-:.]+$"),
]
# ISO-4217 alphabetic code, normalized to upper case.
Currency = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=3, max_length=3, pattern=r"^[A-Za-z]{3}$"),
    AfterValidator(str.upper),
]
# Money is never a float: validated and stored as an exact Decimal.
Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=4)]


class WebhookPayload(BaseModel):
    """Body of POST /v1/webhooks/transactions. Unknown fields from the processor are ignored."""

    model_config = ConfigDict(extra="ignore")

    transaction_id: Identifier
    source_account: Identifier
    destination_account: Identifier
    amount: Amount
    currency: Currency


class TransactionEvent(WebhookPayload):
    """Message on the `transactions.received` topic. Decimal is serialized as a string (lossless)."""

    schema_version: Literal[1] = 1
    received_at: AwareDatetime

    @classmethod
    def from_webhook(cls, payload: WebhookPayload, received_at: datetime) -> "TransactionEvent":
        return cls(**payload.model_dump(), received_at=received_at)
