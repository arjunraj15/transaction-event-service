"""In-memory test doubles that honour the same contracts as the Postgres/Kafka implementations."""

import asyncio
from datetime import UTC, datetime, timedelta

from txn_service.db.models import Transaction, TransactionStatus
from txn_service.events import TxnEvent
from txn_service.messaging.schemas import TransactionEvent


class FakeRepository:
    def __init__(self) -> None:
        self.rows: dict[str, Transaction] = {}
        self.healthy = True

    async def insert_if_absent(self, event: TransactionEvent) -> bool:
        if event.transaction_id in self.rows:
            return False
        self.rows[event.transaction_id] = Transaction(
            transaction_id=event.transaction_id,
            source_account=event.source_account,
            destination_account=event.destination_account,
            amount=event.amount,
            currency=event.currency,
            status=TransactionStatus.PROCESSING,
            created_at=event.received_at,
            processed_at=None,
            attempts=0,
            last_error=None,
        )
        return True

    async def get_status(self, transaction_id: str) -> str | None:
        row = self.rows.get(transaction_id)
        return row.status if row else None

    async def get(self, transaction_id: str) -> Transaction | None:
        return self.rows.get(transaction_id)

    async def list_recent(self, limit: int, status: str | None = None) -> list[Transaction]:
        rows = [r for r in self.rows.values() if status is None or r.status == status]
        return sorted(rows, key=lambda r: r.created_at, reverse=True)[:limit]

    async def ping(self) -> bool:
        return self.healthy

    async def mark_processed(self, transaction_id: str, processed_at: datetime) -> bool:
        row = self.rows[transaction_id]
        if row.status != TransactionStatus.PROCESSING:
            return False
        row.status, row.processed_at, row.attempts = (
            TransactionStatus.PROCESSED,
            processed_at,
            row.attempts + 1,
        )
        return True

    async def record_attempt_error(self, transaction_id: str, error: str) -> None:
        row = self.rows[transaction_id]
        row.attempts, row.last_error = row.attempts + 1, error

    async def mark_failed(self, transaction_id: str, error: str) -> None:
        row = self.rows[transaction_id]
        if row.status == TransactionStatus.PROCESSING:
            row.status, row.last_error = TransactionStatus.FAILED, error


class FakeDeadLetters:
    def __init__(self) -> None:
        self.messages: list[tuple[bytes, bytes | None, str]] = []

    async def send(self, raw: bytes, key: bytes | None, reason: str) -> None:
        self.messages.append((raw, key, reason))


class FakePublisher:
    def __init__(self, error: Exception | None = None) -> None:
        self.events: list[TransactionEvent] = []
        self.error = error
        self.ready = True

    async def publish(self, event: TransactionEvent) -> None:
        if self.error:
            raise self.error
        self.events.append(event)

    async def is_ready(self) -> bool:
        return self.ready


class FakeEventSource:
    """Stands in for EventBroadcaster: each subscriber gets the queued payloads, then end-of-stream."""

    def __init__(self, payloads: list[str] | None = None, connected: bool = True) -> None:
        self.payloads = payloads or []
        self.connected = connected
        self.subscribers: list[asyncio.Queue[str | None]] = []

    def subscribe(self) -> asyncio.Queue[str | None]:
        queue: asyncio.Queue[str | None] = asyncio.Queue()
        for payload in self.payloads:
            queue.put_nowait(payload)
        queue.put_nowait(None)
        self.subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str | None]) -> None:
        self.subscribers.remove(queue)


class FakeEventSink:
    def __init__(self) -> None:
        self.events: list[TxnEvent] = []

    async def emit(self, event: TxnEvent) -> None:
        self.events.append(event)

    @property
    def types(self) -> list[str]:
        return [e.type.value for e in self.events]


class FakeClock:
    """Deterministic time: sleep() advances now() instead of waiting."""

    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2024, 1, 15, 10, 30, tzinfo=UTC)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)
