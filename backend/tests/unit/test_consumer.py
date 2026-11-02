import asyncio
from dataclasses import dataclass

from aiokafka import TopicPartition

from txn_service.worker.consumer import PartitionWorker, retry_forever


@dataclass
class Record:
    offset: int
    value: bytes
    key: bytes | None = None


class RecordingProcessor:
    def __init__(self, fail_ingest_times: int = 0) -> None:
        self.ingested: list[bytes] = []
        self.completed: list[bytes] = []
        self._fail = fail_ingest_times

    async def ingest(self, raw: bytes, key: bytes | None = None) -> bytes | None:
        if self._fail:
            self._fail -= 1
            raise ConnectionError("db down")
        self.ingested.append(raw)
        return None if raw == b"dup" else raw

    async def complete(self, event: bytes) -> None:
        await asyncio.sleep(0.01)
        self.completed.append(event)


async def wait_until(predicate, timeout: float = 5.0) -> None:  # type: ignore[no-untyped-def]
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_commits_in_order_only_after_completion() -> None:
    commits: list[int] = []

    async def commit(tp: TopicPartition, offset: int) -> None:
        commits.append(offset)

    processor = RecordingProcessor()
    worker = PartitionWorker(TopicPartition("t", 0), processor, commit)  # type: ignore[arg-type]
    for i, value in enumerate([b"a", b"dup", b"c"]):
        worker.inbox.put_nowait(Record(offset=i, value=value))

    await wait_until(lambda: len(commits) == 3)
    await worker.stop()
    assert commits == [1, 2, 3]
    assert processor.completed == [b"a", b"c"]  # duplicate is committed without completing


async def test_ingest_is_retried_on_infrastructure_errors() -> None:
    commits: list[int] = []

    async def commit(tp: TopicPartition, offset: int) -> None:
        commits.append(offset)

    processor = RecordingProcessor(fail_ingest_times=1)
    worker = PartitionWorker(TopicPartition("t", 0), processor, commit)  # type: ignore[arg-type]
    worker.inbox.put_nowait(Record(offset=0, value=b"a"))
    await wait_until(lambda: commits == [1])
    await worker.stop()
    assert processor.completed == [b"a"]


async def test_stopped_worker_never_commits_in_flight_message() -> None:
    commits: list[int] = []

    async def commit(tp: TopicPartition, offset: int) -> None:
        commits.append(offset)

    class Slow(RecordingProcessor):
        async def complete(self, event: bytes) -> None:
            await asyncio.sleep(60)

    worker = PartitionWorker(TopicPartition("t", 0), Slow(), commit)  # type: ignore[arg-type]
    worker.inbox.put_nowait(Record(offset=0, value=b"a"))
    await asyncio.sleep(0.05)
    await worker.stop()  # e.g. crash / rebalance mid-delay
    assert commits == []  # will be redelivered


async def test_retry_forever_returns_after_transient_errors() -> None:
    attempts = 0

    async def op() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ConnectionError
        return "ok"

    assert await retry_forever(op, "test") == "ok"
    assert attempts == 2
