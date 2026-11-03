"""Per-message business logic, independent of Kafka plumbing so it can be unit-tested with fakes.

Processing is split in two stages so the row is visible (PROCESSING) as soon as the event is consumed:
  ingest()   -> parse + idempotent insert; returns the event if it still needs processing
  complete() -> wait for the simulated external call, then PROCESSED (or FAILED + DLQ)
Errors raised from either stage are infrastructure errors (DB/Kafka down): the caller retries the
message without committing its offset. Business failures are retried here and then dead-lettered.
"""

import asyncio  # Default sleep implementation.
from collections.abc import Awaitable, Callable  # Type aliases for async callables.
from datetime import UTC, datetime, timedelta  # Timezone-aware datetimes and durations.
from typing import Protocol  # Protocol for structural interfaces.

import structlog  # Used to bind transaction_id to all logs within a block.
from pydantic import ValidationError  # Raised when a message does not match the schema.

from txn_service.config import Settings  # Application settings type.
from txn_service.db.models import TransactionStatus  # Enum of stored transaction statuses.
from txn_service.events import EventSink, NullEventSink, TxnEvent, TxnEventType  # Dashboard events.
from txn_service.logging import get_logger  # Project logger factory.
from txn_service.messaging.schemas import TransactionEvent  # Schema of the Kafka message.
from txn_service.metrics import (  # Prometheus metrics updated during processing.
    END_TO_END_LATENCY,
    PROCESSING_ATTEMPT_ERRORS,
    TRANSACTIONS_FAILED,
    TRANSACTIONS_INGESTED,
    TRANSACTIONS_PROCESSED,
)

# Module-level logger.
log = get_logger(__name__)


# Database operations the processor needs (lets tests supply a fake).
class Repository(Protocol):
    # Insert a new row; returns True if inserted, False if it already existed.
    async def insert_if_absent(self, event: TransactionEvent) -> bool: ...
    # Current status of a transaction, or None if unknown.
    async def get_status(self, transaction_id: str) -> str | None: ...
    # Mark as processed; returns False if it was already finalized.
    async def mark_processed(self, transaction_id: str, processed_at: datetime) -> bool: ...
    # Store the error from one failed attempt.
    async def record_attempt_error(self, transaction_id: str, error: str) -> None: ...
    # Mark the transaction as permanently failed.
    async def mark_failed(self, transaction_id: str, error: str) -> None: ...


# Destination for messages that cannot be processed (lets tests supply a fake).
class DeadLetterSink(Protocol):
    # Publish the raw message with its key and the reason it was rejected.
    async def send(self, raw: bytes, key: bytes | None, reason: str) -> None: ...


# Signature of the downstream call: takes the event, returns nothing, may raise.
ExternalCall = Callable[[TransactionEvent], Awaitable[None]]


async def _noop_external_call(event: TransactionEvent) -> None:
    """Hook for the real downstream call (would pass transaction_id as its idempotency key)."""


# Default clock: current UTC time.
def _utcnow() -> datetime:
    return datetime.now(UTC)


class TransactionProcessor:
    def __init__(
        self,
        # Database access.
        repository: Repository,
        # DLQ publisher.
        dead_letters: DeadLetterSink,
        # Delay, retry and other configuration.
        settings: Settings,
        *,
        # Downstream call; replaceable in tests.
        external_call: ExternalCall = _noop_external_call,
        # Clock; replaceable in tests.
        now: Callable[[], datetime] = _utcnow,
        # Sleep function; tests inject a fast fake.
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        # Optional dashboard event sink.
        events: EventSink | None = None,
    ) -> None:
        self._repo = repository
        self._dlq = dead_letters
        self._settings = settings
        self._external_call = external_call
        self._now = now
        self._sleep = sleep
        # Fall back to a sink that discards events when none is given.
        self._events: EventSink = events or NullEventSink()

    async def ingest(self, raw: bytes, key: bytes | None = None) -> TransactionEvent | None:
        try:
            # Parse and validate the message bytes against the schema.
            event = TransactionEvent.model_validate_json(raw)
        except ValidationError as exc:
            # Poison message: retrying can never succeed, so park it for inspection.
            await self._dlq.send(raw, key, reason=f"invalid_message: {exc.error_count()} errors")
            # Count it as failed.
            TRANSACTIONS_FAILED.inc()
            log.error("invalid_message_dead_lettered")
            # Tell the dashboard (no transaction id is available).
            await self._events.emit(
                TxnEvent(
                    type=TxnEventType.INVALID_MESSAGE,
                    at=self._now(),
                    detail=f"{exc.error_count()} validation errors",
                )
            )
            # None tells the caller there is nothing further to process.
            return None

        # Attach transaction_id to every log line emitted inside the block below.
        bound = structlog.contextvars.bound_contextvars(transaction_id=event.transaction_id)
        with bound:
            # Insert returns True only for a first-time transaction (idempotent write).
            if await self._repo.insert_if_absent(event):
                TRANSACTIONS_INGESTED.labels(result="new").inc()
                log.info("transaction_ingested")
                await self._events.emit(
                    TxnEvent.for_transaction(
                        TxnEventType.INGESTED, event, status=TransactionStatus.PROCESSING
                    )
                )
                # New work: hand it to the completion stage.
                return event

            # Row already existed: look at its state to decide what to do.
            status = await self._repo.get_status(event.transaction_id)
            if status == TransactionStatus.PROCESSING:
                # Redelivery after a crash/rebalance before the offset was committed: finish the job.
                log.info("transaction_resumed")
                await self._events.emit(
                    TxnEvent.for_transaction(TxnEventType.RESUMED, event, status=TransactionStatus.PROCESSING)
                )
                return event
            # Already PROCESSED or FAILED: a true duplicate, so skip it.
            TRANSACTIONS_INGESTED.labels(result="duplicate").inc()
            log.info("duplicate_ignored", status=status)
            await self._events.emit(
                TxnEvent.for_transaction(TxnEventType.DUPLICATE_IGNORED, event, status=status)
            )
            return None

    async def complete(self, event: TransactionEvent) -> None:
        # Bind transaction_id to all logs in this block.
        with structlog.contextvars.bound_contextvars(transaction_id=event.transaction_id):
            # Simulated external API latency, measured from webhook receipt so consumer lag doesn't add to it.
            # Moment at which processing is allowed to finish.
            due = event.received_at + timedelta(seconds=self._settings.processing_delay_seconds)
            # Time still left to wait.
            remaining = (due - self._now()).total_seconds()
            # Only sleep if the due time is still in the future.
            if remaining > 0:
                await self._sleep(remaining)

            # Most recent failure message, kept for the DLQ and DB record.
            last_error = ""
            # Attempts are numbered from 1.
            for attempt in range(1, self._settings.processing_max_attempts + 1):
                try:
                    # Call the downstream service.
                    await self._external_call(event)
                    # Success: leave the loop (skips the for-else below).
                    break
                except Exception as exc:
                    last_error = repr(exc)
                    PROCESSING_ATTEMPT_ERRORS.inc()
                    log.warning("processing_attempt_failed", attempt=attempt, error=last_error)
                    # Persist the error so it is visible on the transaction row.
                    await self._repo.record_attempt_error(event.transaction_id, last_error)
                    # No point waiting after the final attempt.
                    if attempt < self._settings.processing_max_attempts:
                        # Exponential backoff: base, 2x base, 4x base, ...
                        backoff = self._settings.processing_retry_backoff_seconds * 2 ** (attempt - 1)
                        await self._sleep(backoff)
            # Runs only when the loop finished without break, i.e. every attempt failed.
            else:
                # Park the message in the DLQ, with the reason capped at 500 characters.
                await self._dlq.send(
                    event.model_dump_json().encode(),
                    event.transaction_id.encode(),
                    reason=f"max_attempts_exceeded: {last_error}"[:500],
                )
                # Record the permanent failure in the database.
                await self._repo.mark_failed(event.transaction_id, last_error)
                TRANSACTIONS_FAILED.inc()
                log.error("transaction_dead_lettered", error=last_error)
                await self._events.emit(
                    TxnEvent.for_transaction(
                        TxnEventType.DEAD_LETTERED, event, status=TransactionStatus.FAILED, detail=last_error
                    )
                )
                # Done; do not mark as processed.
                return

            # Reached only after a successful external call.
            processed_at = self._now()
            # Returns True only if this call performed the PROCESSING -> PROCESSED transition.
            if await self._repo.mark_processed(event.transaction_id, processed_at):
                TRANSACTIONS_PROCESSED.inc()
                # Seconds from webhook receipt to completion.
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
                # Someone else already finalized it (e.g. a redelivery race); nothing more to do.
                log.info("already_finalized")
