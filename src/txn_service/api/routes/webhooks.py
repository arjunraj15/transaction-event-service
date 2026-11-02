from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from txn_service.api.dependencies import PublisherDep, SettingsDep
from txn_service.api.schemas import WebhookAccepted
from txn_service.api.security import verify_signature
from txn_service.logging import get_logger
from txn_service.messaging.schemas import TransactionEvent, WebhookPayload
from txn_service.metrics import WEBHOOKS_RECEIVED

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
log = get_logger(__name__)


@router.post(
    "/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAccepted,
    responses={401: {"description": "Invalid signature"}, 503: {"description": "Broker unavailable"}},
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": WebhookPayload.model_json_schema()}},
        }
    },
)
async def receive_transaction_webhook(
    request: Request, settings: SettingsDep, publisher: PublisherDep
) -> WebhookAccepted:
    """Acknowledge fast: verify, validate, durably hand off to Kafka, return 202. No processing here."""
    received_at = datetime.now(UTC)
    body = await request.body()  # raw bytes are needed for the HMAC

    if settings.webhook_secret is not None:
        signature = request.headers.get(settings.webhook_signature_header)
        if not verify_signature(settings.webhook_secret.get_secret_value(), body, signature):
            WEBHOOKS_RECEIVED.labels(outcome="invalid_signature").inc()
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook signature")

    try:
        payload = WebhookPayload.model_validate_json(body)
    except ValidationError as exc:
        WEBHOOKS_RECEIVED.labels(outcome="invalid_payload").inc()
        raise RequestValidationError(exc.errors(include_url=False)) from exc

    event = TransactionEvent.from_webhook(payload, received_at)
    try:
        await publisher.publish(event)
    except Exception as exc:
        # Not acknowledged → the processor's retry policy redelivers it; nothing is silently lost.
        WEBHOOKS_RECEIVED.labels(outcome="broker_unavailable").inc()
        log.warning("publish_failed", transaction_id=event.transaction_id, error=repr(exc))
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Temporarily unable to accept webhook, please retry",
            headers={"Retry-After": "1"},
        ) from exc

    WEBHOOKS_RECEIVED.labels(outcome="accepted").inc()
    log.info("webhook_accepted", transaction_id=event.transaction_id)
    return WebhookAccepted(transaction_id=event.transaction_id)
