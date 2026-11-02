"""Live transaction events for the demo dashboard, carried over Postgres LISTEN/NOTIFY.

The worker emits a small JSON event at each state change; every API process keeps one LISTEN
connection and fans events out to its Server-Sent-Events subscribers. Postgres is already shared by
all processes, so no extra infrastructure is needed. Events are best effort and purely informational:
the database row stays the source of truth, and a failed NOTIFY never affects processing.
"""

import asyncio
import contextlib
import enum
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

import asyncpg
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from txn_service.db.engine import normalize_database_url
from txn_service.logging import get_logger
from txn_service.messaging.schemas import TransactionEvent

log = get_logger(__name__)

# Postgres caps a NOTIFY payload at 8000 bytes; stay well under it.
_MAX_DETAIL_CHARS = 500
_KEEPALIVE_SECONDS = 60.0
_MAX_RECONNECT_SECONDS = 30.0
# One round trip for a whole batch of events, delivered in order.
_NOTIFY_BATCH = text("SELECT pg_notify(:channel, payload) FROM unnest(CAST(:payloads AS text[])) AS payload")


class TxnEventType(enum.StrEnum):
    INGESTED = "ingested"
    RESUMED = "resumed"
    DUPLICATE_IGNORED = "duplicate_ignored"
    PROCESSED = "processed"
    DEAD_LETTERED = "dead_lettered"
    INVALID_MESSAGE = "invalid_message"


class TxnEvent(BaseModel):
    type: TxnEventType
    at: datetime
    transaction_id: str | None = None
    status: str | None = None
    amount: Decimal | None = None
    currency: str | None = None
    created_at: datetime | None = None
    processed_at: datetime | None = None
    duration_s: float | None = None
    detail: str | None = None

    @classmethod
    def for_transaction(cls, type_: TxnEventType, event: TransactionEvent, **fields: Any) -> "TxnEvent":
        return cls(
            type=type_,
            at=datetime.now(UTC),
            transaction_id=event.transaction_id,
            amount=event.amount,
            currency=event.currency,
            created_at=event.received_at,
            **fields,
        )

    def to_payload(self) -> str:
        if self.detail and len(self.detail) > _MAX_DETAIL_CHARS:
            self.detail = self.detail[:_MAX_DETAIL_CHARS]
        return self.model_dump_json(exclude_none=True)


class EventSink(Protocol):
    async def emit(self, event: TxnEvent) -> None: ...


class NullEventSink:
    async def emit(self, event: TxnEvent) -> None:
        return None


class PgNotifyEventSink:
    """Worker side: publish events with pg_notify on the shared database.

    Fire-and-forget: `emit` only enqueues, so processing never waits on the event bus. A single
    background drainer sends queued events in order, batching whatever is pending into one round trip.
    Give it its own small engine so it never competes with processing for pooled connections.
    """

    def __init__(self, engine: AsyncEngine, channel: str, *, max_pending: int = 5000) -> None:
        self._engine = engine
        self._channel = channel
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_pending)
        self._drainer: asyncio.Task[None] | None = None
        self.dropped = 0

    async def emit(self, event: TxnEvent) -> None:
        if self._drainer is None:
            self._drainer = asyncio.create_task(self._drain(), name="event-sink")
        try:
            self._queue.put_nowait(event.to_payload())
        except asyncio.QueueFull:
            self.dropped += 1  # bus unreachable for a long time: drop rather than grow memory

    async def _drain(self) -> None:
        while True:
            batch = [await self._queue.get()]
            while not self._queue.empty() and len(batch) < 100:
                batch.append(self._queue.get_nowait())
            try:
                async with self._engine.begin() as conn:
                    await conn.execute(_NOTIFY_BATCH, {"channel": self._channel, "payloads": batch})
            except Exception as exc:  # informational only: never let it affect processing
                log.warning("event_emit_failed", events=len(batch), error=repr(exc))
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def close(self) -> None:
        """Give queued events up to 2 s to go out on shutdown, then stop the drainer."""
        if self._drainer is None:
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(2):
                await self._queue.join()
        self._drainer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._drainer


class EventBroadcaster:
    """API side: one LISTEN connection per process, fanned out to bounded per-client queues.

    A full queue means the client is too slow; it receives a `None` sentinel, its stream ends, and the
    browser's EventSource reconnects (re-seeding from the REST endpoint).
    """

    def __init__(self, database_url: str, channel: str, *, queue_size: int = 500) -> None:
        url, connect_args = normalize_database_url(database_url)
        self._dsn = url.replace("postgresql+asyncpg://", "postgresql://", 1)
        self._ssl = connect_args.get("ssl")
        self._channel = channel
        self._queue_size = queue_size
        self._subscribers: set[asyncio.Queue[str | None]] = set()
        self._task: asyncio.Task[None] | None = None
        self.connected = False

    def subscribe(self) -> asyncio.Queue[str | None]:
        queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str | None]) -> None:
        self._subscribers.discard(queue)

    def publish(self, payload: str) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                self._subscribers.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="event-broadcaster")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        for queue in list(self._subscribers):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)

    def _on_notify(self, _conn: object, _pid: int, _channel: str, payload: str) -> None:
        self.publish(payload)

    async def _run(self) -> None:
        delay = 1.0
        while True:
            conn: asyncpg.Connection | None = None
            try:
                conn = await asyncpg.connect(self._dsn, ssl=self._ssl)
                await conn.add_listener(self._channel, self._on_notify)
                self.connected = True
                delay = 1.0
                log.info("event_listener_connected", channel=self._channel)
                while True:  # keepalive doubles as liveness detection for the LISTEN connection
                    await asyncio.sleep(_KEEPALIVE_SECONDS)
                    await conn.execute("SELECT 1")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("event_listener_disconnected", error=repr(exc), retry_in=delay)
            finally:
                self.connected = False
                if conn is not None:
                    with contextlib.suppress(Exception):
                        await conn.close(timeout=2)
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_SECONDS)


def sse_frame(payload: str, event: str | None = None) -> str:
    lines = [f"event: {event}"] if event else []
    lines += [f"data: {line}" for line in payload.splitlines() or [""]]
    return "\n".join(lines) + "\n\n"


def hello_payload(connected: bool) -> str:
    return json.dumps({"connected": connected, "at": datetime.now(UTC).isoformat()})
