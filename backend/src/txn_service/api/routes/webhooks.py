# UTC-aware timestamps.
from datetime import UTC, datetime

# Router, errors, raw request access and status codes.
from fastapi import APIRouter, HTTPException, Request, status

# Raising this yields the standard 422 validation error response.
from fastapi.exceptions import RequestValidationError

# Error type raised when manual validation fails.
from pydantic import ValidationError

# Dependencies for the publisher and settings.
from txn_service.api.dependencies import PublisherDep, SettingsDep

# Response model.
from txn_service.api.schemas import WebhookAccepted

# Signature verification helper.
from txn_service.api.security import verify_signature

# Structured logger factory.
from txn_service.logging import get_logger

# Payload and event models.
from txn_service.messaging.schemas import TransactionEvent, WebhookPayload

# Prometheus counter of webhook outcomes.
from txn_service.metrics import WEBHOOKS_RECEIVED

# Routes live under /v1/webhooks.
router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
# Module logger.
log = get_logger(__name__)


# POST /v1/webhooks/transactions.
@router.post(
    # Path of the endpoint.
    "/transactions",
    # Respond 202: accepted for asynchronous processing.
    status_code=status.HTTP_202_ACCEPTED,
    # Shape of the success body.
    response_model=WebhookAccepted,
    # Document the error responses in OpenAPI.
    responses={401: {"description": "Invalid signature"}, 503: {"description": "Broker unavailable"}},
    # Override the request body schema because the body is parsed manually.
    openapi_extra={
        # Describe the request body.
        "requestBody": {
            # Body is mandatory.
            "required": True,
            # Use the payload model's JSON schema.
            "content": {"application/json": {"schema": WebhookPayload.model_json_schema()}},
        }
    },
)
# Handler gets the raw request plus settings and publisher.
async def receive_transaction_webhook(
    request: Request, settings: SettingsDep, publisher: PublisherDep
) -> WebhookAccepted:
    """Acknowledge fast: verify, validate, durably hand off to Kafka, return 202. No processing here."""
    # Timestamp the arrival before doing any work.
    received_at = datetime.now(UTC)
    body = await request.body()  # raw bytes are needed for the HMAC

    # Verify the signature only when a secret is configured.
    if settings.webhook_secret is not None:
        # Read the signature from the configured header.
        signature = request.headers.get(settings.webhook_signature_header)
        # Compare it with the HMAC of the body.
        if not verify_signature(settings.webhook_secret.get_secret_value(), body, signature):
            # Count the rejected request.
            WEBHOOKS_RECEIVED.labels(outcome="invalid_signature").inc()
            # Reject with 401.
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook signature")

    # Parse and validate the body manually (after the signature check).
    try:
        # Validate the JSON against the payload model.
        payload = WebhookPayload.model_validate_json(body)
    # Invalid payload.
    except ValidationError as exc:
        # Count the invalid payload.
        WEBHOOKS_RECEIVED.labels(outcome="invalid_payload").inc()
        # Re-raise as a standard 422 error without leaking internal URLs.
        raise RequestValidationError(exc.errors(include_url=False)) from exc

    # Build the Kafka event from the payload and arrival time.
    event = TransactionEvent.from_webhook(payload, received_at)
    # Hand off to Kafka.
    try:
        # Wait for the broker's acknowledgement.
        await publisher.publish(event)
    # Any publish failure (including timeout).
    except Exception as exc:
        # Not acknowledged → the processor's retry policy redelivers it; nothing is silently lost.
        # Count the failure.
        WEBHOOKS_RECEIVED.labels(outcome="broker_unavailable").inc()
        # Log it with the transaction id for debugging.
        log.warning("publish_failed", transaction_id=event.transaction_id, error=repr(exc))
        # Return 503 so the sender retries.
        raise HTTPException(
            # Use the 503 constant.
            status.HTTP_503_SERVICE_UNAVAILABLE,
            # Message for the client.
            detail="Temporarily unable to accept webhook, please retry",
            # Suggest retrying after one second.
            headers={"Retry-After": "1"},
        ) from exc

    # Count the accepted webhook.
    WEBHOOKS_RECEIVED.labels(outcome="accepted").inc()
    # Log the acceptance.
    log.info("webhook_accepted", transaction_id=event.transaction_id)
    # Return the 202 body with the transaction id.
    return WebhookAccepted(transaction_id=event.transaction_id)
