"""Kafka consumption runtime: concurrency, ordering, backpressure and offset management.

Each assigned partition gets two sequential tasks connected by a FIFO:
  ingest   : inbox   -> processor.ingest()   -> pending   (row inserted immediately on consume)
  complete : pending -> processor.complete() -> commit offset+1
Partitions are independent, order within a partition is preserved, and an offset is committed only
after its message is fully processed, so a crash at any point leads to redelivery (never loss) and
redelivery is safe because processing is idempotent.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError

from txn_service.logging import get_logger
from txn_service.messaging.schemas import TransactionEvent
from txn_service.worker.processor import TransactionProcessor

log = get_logger(__name__)
MAX_INFRA_BACKOFF_SECONDS = 30.0


async def retry_forever[T](operation: Callable[[], Awaitable[T]], what: str) -> T:
    """Infrastructure failures (DB/broker down) must not drop messages: back off and retry."""
    delay = 0.5
    while True:
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("infra_error_retrying", operation=what, error=repr(exc), retry_in=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_INFRA_BACKOFF_SECONDS)


class PartitionWorker:
    def __init__(
        self,
        tp: TopicPartition,
        processor: TransactionProcessor,
        commit: Callable[[TopicPartition, int], Awaitable[None]],
    ) -> None:
        self.tp = tp
        self._processor = processor
        self._commit = commit
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._pending: asyncio.Queue[tuple[Any, TransactionEvent | None]] = asyncio.Queue()
        self._tasks = [
            asyncio.create_task(self._ingest_loop(), name=f"ingest-{tp.partition}"),
            asyncio.create_task(self._complete_loop(), name=f"complete-{tp.partition}"),
        ]

    @property
    def backlog(self) -> int:
        return self.inbox.qsize() + self._pending.qsize()

    async def _ingest_loop(self) -> None:
        while True:
            record = await self.inbox.get()
            event = await retry_forever(partial(self._processor.ingest, record.value, record.key), "ingest")
            await self._pending.put((record, event))

    async def _complete_loop(self) -> None:
        while True:
            record, event = await self._pending.get()
            if event is not None:
                await retry_forever(partial(self._processor.complete, event), "complete")
            await self._commit(self.tp, record.offset + 1)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)


class ConsumerRunner:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        processor: TransactionProcessor,
        topic: str,
        *,
        max_backlog_per_partition: int = 1000,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._topic = topic
        self._max_backlog = max_backlog_per_partition
        self._workers: dict[TopicPartition, PartitionWorker] = {}
        self._paused: set[TopicPartition] = set()
        self.last_poll_monotonic = time.monotonic()

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        try:
            await self._consumer.commit({tp: offset})
        except (CommitFailedError, KafkaError) as exc:
            # Typically a rebalance: the new owner re-reads from the last commit; idempotency makes it safe.
            log.warning("commit_failed", partition=tp.partition, offset=offset, error=repr(exc))

    async def _drop_workers(self, partitions: set[TopicPartition]) -> None:
        for tp in partitions:
            worker = self._workers.pop(tp, None)
            self._paused.discard(tp)
            if worker is not None:
                await worker.stop()

    def _worker_for(self, tp: TopicPartition) -> PartitionWorker:
        worker = self._workers.get(tp)
        if worker is None:
            worker = self._workers[tp] = PartitionWorker(tp, self._processor, self._commit)
        return worker

    def _apply_backpressure(self) -> None:
        """Pause fetching for partitions with a large in-memory backlog; resume when drained."""
        for tp, worker in self._workers.items():
            if tp not in self._paused and worker.backlog >= self._max_backlog:
                self._consumer.pause(tp)
                self._paused.add(tp)
            elif tp in self._paused and worker.backlog <= self._max_backlog // 2:
                self._consumer.resume(tp)
                self._paused.discard(tp)

    async def run(self, stop: asyncio.Event) -> None:
        runner = self

        class Listener(ConsumerRebalanceListener):  # type: ignore[misc]
            async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
                log.info("partitions_revoked", partitions=sorted(tp.partition for tp in revoked))
                await runner._drop_workers(set(revoked))

            async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
                log.info("partitions_assigned", partitions=sorted(tp.partition for tp in assigned))

        self._consumer.subscribe([self._topic], listener=Listener())
        log.info("consumer_started", topic=self._topic)
        try:
            while not stop.is_set():
                batches = await self._consumer.getmany(timeout_ms=1000)
                self.last_poll_monotonic = time.monotonic()
                for tp, records in batches.items():
                    worker = self._worker_for(tp)
                    for record in records:
                        worker.inbox.put_nowait(record)
                self._apply_backpressure()
        finally:
            # Uncommitted in-flight messages are simply redelivered after restart.
            await self._drop_workers(set(self._workers))
            log.info("consumer_stopped")
