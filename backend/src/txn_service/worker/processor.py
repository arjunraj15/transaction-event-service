"""Per-message business logic, independent of Kafka plumbing so it can be unit-tested with fakes.

Processing is split in two stages so the row is visible (PROCESSING) as soon as the event is consumed:
  ingest()   -> parse + idempotent insert; returns the event if it still needs processing
  complete() -> wait for the simulated external call, then PROCESSED (or FAILED + DLQ)
Errors raised from either stage are infrastructure errors (DB/Kafka down): the caller retries the
message without committing its offset. Business failures are retried here and then dead-lettered.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

import structlog
from pydantic import ValidationError

from txn_service.config import Settings
from txn_service.db.models import TransactionStatus
from txn_service.events import EventSink, NullEventSink, TxnEvent, TxnEventType
from txn_service.logging import get_logger
from txn_service.messaging.schemas import TransactionEvent
from txn_service.metrics import (
    END_TO_END_LATENCY,
    PROCESSING_ATTEMPT_ERRORS,
    TRANSACTIONS_FAILED,
    TRANSACTIONS_INGESTED,
    TRANSACTIONS_PROCESSED,
)

log = get_logger(__name__)


class Repository(Protocol):
    async def insert_if_absent(self, event: TransactionEvent) -> bool: ...
    async def get_status(self, transaction_id: str) -> str | None: ...
    async def mark_processed(self, transaction_id: str, processed_at: datetime) -> bool: ...
    async def record_attempt_error(self, transaction_id: str, error: str) -> None: ...
    async def mark_failed(self, transaction_id: str, error: str) -> None: ...


class DeadLetterSink(Protocol):
    async def send(self, raw: bytes, key: bytes | None, reason: str) -> None: ...


ExternalCall = Callable[[TransactionEvent], Awaitable[None]]


async def _noop_external_call(event: TransactionEvent) -> None:
    """Hook for the real downstream call (would pass transaction_id as its idempotency key)."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


class TransactionProcessor:
    def __init__(
        self,
        repository: Repository,
        dead_letters: DeadLetterSink,
        settings: Settings,
        *,
        external_call: ExternalCall = _noop_external_call,
        now: Callable[[], datetime] = _utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        events: EventSink | None = None,
    ) -> None:
        self._repo = repository
        self._dlq = dead_letters
        self._settings = settings
        self._external_call = external_call
        self._now = now
        self._sleep = sleep
        self._events: EventSink = events or NullEventSink()

    async def ingest(self, raw: bytes, key: bytes | None = None) -> TransactionEvent | None:
        try:
            event = TransactionEvent.model_validate_json(raw)
        except ValidationError as exc:
            # Poison message: retrying can never succeed, so park it for inspection.
            await self._dlq.send(raw, key, reason=f"invalid_message: {exc.error_count()} errors")
            TRANSACTIONS_FAILED.inc()
            log.error("invalid_message_dead_lettered")
            await self._events.emit(
                TxnEvent(
                    type=TxnEventType.INVALID_MESSAGE,
                    at=self._now(),
                    detail=f"{exc.error_count()} validation errors",
                )
            )
            return None

        bound = structlog.contextvars.bound_contextvars(transaction_id=event.transaction_id)
        with bound:
            if await self._repo.insert_if_absent(event):
                TRANSACTIONS_INGESTED.labels(result="new").inc()
                log.info("transaction_ingested")
                await self._events.emit(
                    TxnEvent.for_transaction(
                        TxnEventType.INGESTED, event, status=TransactionStatus.PROCESSING
                    )
                )
                return event

            status = await self._repo.get_status(event.transaction_id)
            if status == TransactionStatus.PROCESSING:
                # Redelivery after a crash/rebalance before the offset was committed: finish the job.
                log.info("transaction_resumed")
                await self._events.emit(
                    TxnEvent.for_transaction(TxnEventType.RESUMED, event, status=TransactionStatus.PROCESSING)
                )
                return event
            TRANSACTIONS_INGESTED.labels(result="duplicate").inc()
            log.info("duplicate_ignored", status=status)
            await self._events.emit(
                TxnEvent.for_transaction(TxnEventType.DUPLICATE_IGNORED, event, status=status)
            )
            return None

    async def complete(self, event: TransactionEvent) -> None:
        with structlog.contextvars.bound_contextvars(transaction_id=event.transaction_id):
            # Simulated external API latency, measured from webhook receipt so consumer lag doesn't add to it.
            due = event.received_at + timedelta(seconds=self._settings.processing_delay_seconds)
            remaining = (due - self._now()).total_seconds()
            if remaining > 0:
                await self._sleep(remaining)

            last_error = ""
            for attempt in range(1, self._settings.processing_max_attempts + 1):
                try:
                    await self._external_call(event)
                    break
                except Exception as exc:
                    last_error = repr(exc)
                    PROCESSING_ATTEMPT_ERRORS.inc()
                    log.warning("processing_attempt_failed", attempt=attempt, error=last_error)
                    await self._repo.record_attempt_error(event.transaction_id, last_error)
                    if attempt < self._settings.processing_max_attempts:
                        backoff = self._settings.processing_retry_backoff_seconds * 2 ** (attempt - 1)
                        await self._sleep(backoff)
            else:
                await self._dlq.send(
                    event.model_dump_json().encode(),
                    event.transaction_id.encode(),
                    reason=f"max_attempts_exceeded: {last_error}"[:500],
                )
                await self._repo.mark_failed(event.transaction_id, last_error)
                TRANSACTIONS_FAILED.inc()
                log.error("transaction_dead_lettered", error=last_error)
                await self._events.emit(
                    TxnEvent.for_transaction(
                        TxnEventType.DEAD_LETTERED, event, status=TransactionStatus.FAILED, detail=last_error
                    )
                )
                return

            processed_at = self._now()
            if await self._repo.mark_processed(event.transaction_id, processed_at):
                TRANSACTIONS_PROCESSED.inc()
                duration = (processed_at - event.received_at).total_seconds()
                END_TO_END_LATENCY.observe(duration)
                log.info("transaction_processed")
                await self._events.emit(
                    TxnEvent.for_transaction(
                        TxnEventType.PROCESSED,
                        event,
                        status=TransactionStatus.PROCESSED,
                        processed_at=processed_at,
                        duration_s=round(duration, 3),
                    )
                )
            else:
                log.info("already_finalized")
