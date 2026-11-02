from datetime import UTC, datetime, timedelta

import pytest

from tests.fakes import FakeClock, FakeDeadLetters, FakeRepository
from txn_service.config import Settings
from txn_service.messaging.schemas import TransactionEvent
from txn_service.worker.processor import TransactionProcessor


def make_event(clock: FakeClock, txn_id: str = "txn_1") -> TransactionEvent:
    return TransactionEvent(
        transaction_id=txn_id,
        source_account="acc_a",
        destination_account="acc_b",
        amount="1500",  # type: ignore[arg-type]
        currency="INR",
        received_at=clock.now(),
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def repo() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def dlq() -> FakeDeadLetters:
    return FakeDeadLetters()


def make_processor(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock, **kwargs: object
) -> TransactionProcessor:
    settings = Settings(processing_delay_seconds=30)
    return TransactionProcessor(repo, dlq, settings, now=clock.now, sleep=clock.sleep, **kwargs)  # type: ignore[arg-type]


async def test_new_transaction_is_processed_after_30s(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    processor = make_processor(repo, dlq, clock)
    event = make_event(clock)
    clock.current += timedelta(seconds=2)  # simulate 2s consumer lag

    prepared = await processor.ingest(event.model_dump_json().encode())
    assert prepared is not None
    assert repo.rows["txn_1"].status == "PROCESSING"

    await processor.complete(prepared)
    row = repo.rows["txn_1"]
    assert row.status == "PROCESSED"
    assert clock.sleeps == [28.0]  # delay is measured from webhook receipt, lag is not added
    assert row.processed_at - row.created_at == timedelta(seconds=30)
    assert dlq.messages == []


async def test_duplicate_of_processed_transaction_is_skipped(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    processor = make_processor(repo, dlq, clock)
    raw = make_event(clock).model_dump_json().encode()
    first = await processor.ingest(raw)
    assert first is not None
    await processor.complete(first)

    assert await processor.ingest(raw) is None
    assert repo.rows["txn_1"].attempts == 1


async def test_redelivery_of_in_flight_transaction_resumes(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    processor = make_processor(repo, dlq, clock)
    raw = make_event(clock).model_dump_json().encode()
    assert await processor.ingest(raw) is not None
    # crash before complete(): offset not committed, message redelivered
    resumed = await processor.ingest(raw)
    assert resumed is not None
    await processor.complete(resumed)
    assert repo.rows["txn_1"].status == "PROCESSED"


async def test_complete_is_idempotent(repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock) -> None:
    processor = make_processor(repo, dlq, clock)
    event = await processor.ingest(make_event(clock).model_dump_json().encode())
    assert event is not None
    await processor.complete(event)
    first_processed_at = repo.rows["txn_1"].processed_at
    await processor.complete(event)
    assert repo.rows["txn_1"].processed_at == first_processed_at
    assert repo.rows["txn_1"].attempts == 1


async def test_poison_message_is_dead_lettered(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    processor = make_processor(repo, dlq, clock)
    assert await processor.ingest(b"{not json", b"k") is None
    [(raw, key, reason)] = dlq.messages
    assert raw == b"{not json" and key == b"k" and reason.startswith("invalid_message")
    assert repo.rows == {}


async def test_permanent_failure_retries_then_dead_letters(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    calls = 0

    async def always_fails(event: TransactionEvent) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("downstream 500")

    processor = make_processor(repo, dlq, clock, external_call=always_fails)
    event = await processor.ingest(make_event(clock).model_dump_json().encode())
    assert event is not None
    await processor.complete(event)

    assert calls == 3
    row = repo.rows["txn_1"]
    assert row.status == "FAILED" and row.attempts == 3 and "downstream 500" in (row.last_error or "")
    assert len(dlq.messages) == 1
    assert clock.sleeps == [30.0, 1.0, 2.0]  # delay, then exponential backoff


async def test_transient_failure_recovers(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    calls = 0

    async def flaky(event: TransactionEvent) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("timeout")

    processor = make_processor(repo, dlq, clock, external_call=flaky)
    event = await processor.ingest(make_event(clock).model_dump_json().encode())
    assert event is not None
    await processor.complete(event)
    assert repo.rows["txn_1"].status == "PROCESSED"
    assert dlq.messages == []


async def test_live_events_are_emitted_for_each_state_change(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    from tests.fakes import FakeEventSink

    sink = FakeEventSink()
    processor = make_processor(repo, dlq, clock, events=sink)
    raw = make_event(clock).model_dump_json().encode()

    event = await processor.ingest(raw)
    assert event is not None
    await processor.complete(event)
    assert await processor.ingest(raw) is None  # duplicate
    assert await processor.ingest(b"{bad", b"k") is None  # poison

    assert sink.types == ["ingested", "processed", "duplicate_ignored", "invalid_message"]
    ingested, processed = sink.events[0], sink.events[1]
    assert ingested.transaction_id == "txn_1" and ingested.status == "PROCESSING"
    assert str(ingested.amount) == "1500" and ingested.currency == "INR"
    assert processed.status == "PROCESSED" and processed.duration_s == 30.0


async def test_failed_event_sink_never_breaks_processing(
    repo: FakeRepository, dlq: FakeDeadLetters, clock: FakeClock
) -> None:
    from txn_service.events import PgNotifyEventSink

    class BrokenEngine:
        def begin(self) -> object:
            raise ConnectionError("db down")

    sink = PgNotifyEventSink(BrokenEngine(), "txn_events")  # type: ignore[arg-type]
    processor = make_processor(repo, dlq, clock, events=sink)
    event = await processor.ingest(make_event(clock).model_dump_json().encode())
    assert event is not None
    await processor.complete(event)
    assert repo.rows["txn_1"].status == "PROCESSED"
    await sink.close()  # the drainer logged the failure; nothing propagated


async def test_pg_notify_sink_never_blocks_the_caller() -> None:
    import asyncio

    from txn_service.events import PgNotifyEventSink, TxnEvent, TxnEventType

    release = asyncio.Event()
    sent: list[list[str]] = []

    class SlowConn:
        async def execute(self, _stmt: object, params: dict[str, list[str]]) -> None:
            await release.wait()  # e.g. a slow or unreachable database
            sent.append(params["payloads"])

    class SlowEngine:
        def begin(self) -> object:
            class Ctx:
                async def __aenter__(self) -> SlowConn:
                    return SlowConn()

                async def __aexit__(self, *_: object) -> None:
                    return None

            return Ctx()

    sink = PgNotifyEventSink(SlowEngine(), "txn_events")  # type: ignore[arg-type]
    at = datetime(2024, 1, 1, tzinfo=UTC)
    async with asyncio.timeout(0.5):  # would hang if emit awaited the database
        for i in range(3):
            await sink.emit(TxnEvent(type=TxnEventType.INGESTED, at=at, transaction_id=f"t{i}"))
    release.set()
    await sink.close()
    assert [p for batch in sent for p in batch] and len([p for batch in sent for p in batch]) == 3
