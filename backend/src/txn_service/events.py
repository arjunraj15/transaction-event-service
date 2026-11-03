"""Live transaction events for the demo dashboard, carried over Postgres LISTEN/NOTIFY.

The worker emits a small JSON event at each state change; every API process keeps one LISTEN
connection and fans events out to its Server-Sent-Events subscribers. Postgres is already shared by
all processes, so no extra infrastructure is needed. Events are best effort and purely informational:
the database row stays the source of truth, and a failed NOTIFY never affects processing.
"""

import asyncio  # Async primitives: queues, tasks, timeouts.
import contextlib  # suppress() lets us ignore specific expected exceptions concisely.
import enum  # Base for the string-valued event type enum.
import json  # Serialise the hello payload.
from datetime import UTC, datetime  # Timezone-aware timestamps.
from decimal import Decimal  # Exact decimal type for money amounts.
from typing import Any, Protocol  # Any for free-form kwargs; Protocol for the structural sink interface.

import asyncpg  # Raw driver, for LISTEN.
from pydantic import BaseModel  # Base class for validated, serialisable event models.
from sqlalchemy import text  # text() wraps a raw SQL string for SQLAlchemy.
from sqlalchemy.ext.asyncio import AsyncEngine  # Async engine type, used for type hints.

from txn_service.db.engine import normalize_database_url  # URL to asyncpg form.
from txn_service.logging import get_logger  # Project logger factory.
from txn_service.messaging.schemas import TransactionEvent  # Kafka message schema.

# Module-level logger named after this module.
log = get_logger(__name__)

# Postgres caps a NOTIFY payload at 8000 bytes; stay well under it.
# Longest `detail` text we will send.
_MAX_DETAIL_CHARS = 500
# Idle seconds between liveness pings on the LISTEN connection.
_KEEPALIVE_SECONDS = 60.0
# Upper bound for the exponential reconnect delay.
_MAX_RECONNECT_SECONDS = 30.0
# One round trip for a whole batch of events, delivered in order.
_NOTIFY_BATCH = text("SELECT pg_notify(:channel, payload) FROM unnest(CAST(:payloads AS text[])) AS payload")


# The kinds of state change the dashboard can display.
class TxnEventType(enum.StrEnum):
    # New transaction row was inserted.
    INGESTED = "ingested"
    # Redelivered message for a transaction still being processed.
    RESUMED = "resumed"
    # Message for a transaction that was already finished.
    DUPLICATE_IGNORED = "duplicate_ignored"
    # Transaction completed successfully.
    PROCESSED = "processed"
    # Transaction exhausted its retries and went to the DLQ.
    DEAD_LETTERED = "dead_lettered"
    # Message could not be parsed and went to the DLQ.
    INVALID_MESSAGE = "invalid_message"


# One event as sent to the dashboard; every field except type/at is optional.
class TxnEvent(BaseModel):
    # What happened.
    type: TxnEventType
    # When the event was emitted.
    at: datetime
    # Business identifier of the transaction (absent for unparseable messages).
    transaction_id: str | None = None
    # Transaction status after the change.
    status: str | None = None
    # Transaction amount.
    amount: Decimal | None = None
    # Currency code of the amount.
    currency: str | None = None
    # When the webhook was first received.
    created_at: datetime | None = None
    # When processing finished.
    processed_at: datetime | None = None
    # Seconds from receipt to completion.
    duration_s: float | None = None
    # Free-text extra info such as an error message.
    detail: str | None = None

    # Convenience constructor that fills the common fields from a transaction event.
    @classmethod
    def for_transaction(cls, type_: TxnEventType, event: TransactionEvent, **fields: Any) -> "TxnEvent":
        # Build the model, copying identity fields from the source event.
        return cls(
            # Event kind chosen by the caller.
            type=type_,
            # Stamp with the current UTC time.
            at=datetime.now(UTC),
            # Identity and money fields come from the Kafka event.
            transaction_id=event.transaction_id,
            amount=event.amount,
            currency=event.currency,
            # The transaction's creation time is when the webhook was received.
            created_at=event.received_at,
            # Caller-supplied extras (status, detail, processed_at, ...).
            **fields,
        )

    # Serialise to the JSON string sent through NOTIFY.
    def to_payload(self) -> str:
        # Truncate overlong detail so the payload stays under Postgres's size limit.
        if self.detail and len(self.detail) > _MAX_DETAIL_CHARS:
            # Keep only the first _MAX_DETAIL_CHARS characters.
            self.detail = self.detail[:_MAX_DETAIL_CHARS]
        # Omit unset fields to keep the payload small.
        return self.model_dump_json(exclude_none=True)


# Interface for anything that can receive events (structural typing, no inheritance needed).
class EventSink(Protocol):
    # Deliver one event; implementations must not raise into the caller.
    async def emit(self, event: TxnEvent) -> None: ...


# Do-nothing sink used when events are disabled or in tests.
class NullEventSink:
    # Accept and discard the event.
    async def emit(self, event: TxnEvent) -> None:
        return None


class PgNotifyEventSink:
    """Worker side: publish events with pg_notify on the shared database.

    Fire-and-forget: `emit` only enqueues, so processing never waits on the event bus. A single
    background drainer sends queued events in order, batching whatever is pending into one round trip.
    Give it its own small engine so it never competes with processing for pooled connections.
    """

    def __init__(self, engine: AsyncEngine, channel: str, *, max_pending: int = 5000) -> None:
        # Dedicated engine used only for sending notifications.
        self._engine = engine
        # Postgres channel name that listeners subscribe to.
        self._channel = channel
        # Bounded buffer of serialised events waiting to be sent.
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_pending)
        # Background sender task; started lazily on the first emit.
        self._drainer: asyncio.Task[None] | None = None
        # Count of events discarded because the buffer was full.
        self.dropped = 0

    async def emit(self, event: TxnEvent) -> None:
        # Start the background sender on first use (needs a running event loop).
        if self._drainer is None:
            self._drainer = asyncio.create_task(self._drain(), name="event-sink")
        try:
            # Non-blocking enqueue so callers never wait on the database.
            self._queue.put_nowait(event.to_payload())
        except asyncio.QueueFull:
            self.dropped += 1  # bus unreachable for a long time: drop rather than grow memory

    async def _drain(self) -> None:
        # Run until cancelled by close().
        while True:
            # Wait for at least one event.
            batch = [await self._queue.get()]
            # Opportunistically gather more already-queued events, up to 100 per round trip.
            while not self._queue.empty() and len(batch) < 100:
                batch.append(self._queue.get_nowait())
            try:
                # Short transaction; NOTIFYs are delivered when it commits.
                async with self._engine.begin() as conn:
                    # Send the whole batch in one statement, preserving order.
                    await conn.execute(_NOTIFY_BATCH, {"channel": self._channel, "payloads": batch})
            except Exception as exc:  # informational only: never let it affect processing
                # Log and move on; the batch is lost, which is acceptable for best-effort events.
                log.warning("event_emit_failed", events=len(batch), error=repr(exc))
            finally:
                # Mark every item done (success or not) so close() can join the queue.
                for _ in batch:
                    self._queue.task_done()

    async def close(self) -> None:
        """Give queued events up to 2 s to go out on shutdown, then stop the drainer."""
        # Nothing was ever emitted, so there is nothing to flush.
        if self._drainer is None:
            return
        # A timeout here just means we stop waiting.
        with contextlib.suppress(TimeoutError):
            # Cap the flush wait at 2 seconds.
            async with asyncio.timeout(2):
                # Block until every queued event has been processed.
                await self._queue.join()
        # Stop the background sender.
        self._drainer.cancel()
        # Awaiting a cancelled task raises CancelledError; swallow it.
        with contextlib.suppress(asyncio.CancelledError):
            await self._drainer


class EventBroadcaster:
    """API side: one LISTEN connection per process, fanned out to bounded per-client queues.

    A full queue means the client is too slow; it receives a `None` sentinel, its stream ends, and the
    browser's EventSource reconnects (re-seeding from the REST endpoint).
    """

    def __init__(self, database_url: str, channel: str, *, queue_size: int = 500) -> None:
        # Normalise the URL and get driver-specific connect args (e.g. SSL settings).
        url, connect_args = normalize_database_url(database_url)
        # asyncpg.connect wants a plain postgresql:// DSN, not the SQLAlchemy driver form.
        self._dsn = url.replace("postgresql+asyncpg://", "postgresql://", 1)
        # SSL context/setting to pass straight to asyncpg (None if not required).
        self._ssl = connect_args.get("ssl")
        # Channel to LISTEN on.
        self._channel = channel
        # Capacity of each subscriber's queue.
        self._queue_size = queue_size
        # Currently connected SSE clients, each with its own queue.
        self._subscribers: set[asyncio.Queue[str | None]] = set()
        # Background task that maintains the LISTEN connection.
        self._task: asyncio.Task[None] | None = None
        # True while the LISTEN connection is up (reported to clients).
        self.connected = False

    def subscribe(self) -> asyncio.Queue[str | None]:
        # Each client gets a bounded queue so one slow client cannot use unbounded memory.
        queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=self._queue_size)
        # Register it to receive future events.
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str | None]) -> None:
        # discard() is a no-op if it was already removed (e.g. dropped for being slow).
        self._subscribers.discard(queue)

    def publish(self, payload: str) -> None:
        # Iterate over a copy because we may remove subscribers inside the loop.
        for queue in list(self._subscribers):
            try:
                # Never block the listener on a slow client.
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                # Too slow: stop sending to it.
                self._subscribers.discard(queue)
                # Empty the queue to make room for the sentinel.
                while not queue.empty():
                    queue.get_nowait()
                # None tells the client's stream to end so the browser reconnects.
                queue.put_nowait(None)

    async def start(self) -> None:
        # Launch the connection loop in the background.
        self._task = asyncio.create_task(self._run(), name="event-broadcaster")

    async def stop(self) -> None:
        # Stop the listener loop if it was started.
        if self._task is not None:
            self._task.cancel()
            # Wait for it to finish; the CancelledError is expected.
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        # Tell every remaining client its stream is over.
        for queue in list(self._subscribers):
            # If a queue is full, skip it; shutdown is happening anyway.
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)

    def _on_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        # asyncpg listener callback: forward the payload to all subscribers (other args unused).
        self.publish(payload)

    async def _run(self) -> None:
        # Current reconnect delay, reset after each successful connection.
        delay = 1.0
        # Reconnect forever.
        while True:
            # Track the connection so the finally block can close it.
            conn: asyncpg.Connection | None = None
            try:
                # Open a dedicated connection for LISTEN.
                conn = await asyncpg.connect(self._dsn, ssl=self._ssl)
                # Subscribe; _on_notify is called for every NOTIFY on the channel.
                await conn.add_listener(self._channel, self._on_notify)
                # Report healthy to clients.
                self.connected = True
                # A good connection resets the backoff.
                delay = 1.0
                log.info("event_listener_connected", channel=self._channel)
                while True:  # keepalive doubles as liveness detection for the LISTEN connection
                    # Wait between pings.
                    await asyncio.sleep(_KEEPALIVE_SECONDS)
                    # Raises if the connection died, which triggers a reconnect.
                    await conn.execute("SELECT 1")
            except asyncio.CancelledError:
                # Let shutdown cancellation propagate instead of being treated as a failure.
                raise
            except Exception as exc:
                # Any other error: log it and fall through to the retry delay.
                log.warning("event_listener_disconnected", error=repr(exc), retry_in=delay)
            finally:
                # Connection is down (or being torn down).
                self.connected = False
                if conn is not None:
                    # Best-effort close; ignore errors from an already-broken connection.
                    with contextlib.suppress(Exception):
                        await conn.close(timeout=2)
            # Wait before reconnecting.
            await asyncio.sleep(delay)
            # Double the delay for the next failure, capped at the maximum.
            delay = min(delay * 2, _MAX_RECONNECT_SECONDS)


# Format a payload as a Server-Sent-Events frame.
def sse_frame(payload: str, event: str | None = None) -> str:
    # Optional "event:" line names the event type for the browser.
    lines = [f"event: {event}"] if event else []
    # Each payload line needs its own "data:" prefix; `or [""]` keeps one line for empty payloads.
    lines += [f"data: {line}" for line in payload.splitlines() or [""]]
    # A blank line (double newline) terminates the frame.
    return "\n".join(lines) + "\n\n"


# Build the initial message telling a new client whether the listener is up.
def hello_payload(connected: bool) -> str:
    # Include a timestamp so clients can tell when the status was generated.
    return json.dumps({"connected": connected, "at": datetime.now(UTC).isoformat()})
