"""Kafka consumption runtime: concurrency, ordering, backpressure and offset management.

Each assigned partition gets two sequential tasks connected by a FIFO:
  ingest   : inbox   -> processor.ingest()   -> pending   (row inserted immediately on consume)
  complete : pending -> processor.complete() -> commit offset+1
Partitions are independent, order within a partition is preserved, and an offset is committed only
after its message is fully processed, so a crash at any point leads to redelivery (never loss) and
redelivery is safe because processing is idempotent.
"""

import asyncio  # Async tasks, queues and events.
import time  # Monotonic clock for measuring poll staleness.
from collections.abc import Awaitable, Callable  # Type aliases for async callables.
from functools import partial  # partial binds arguments to a function so it can be retried with no args.
from typing import Any  # Any for Kafka record objects we do not type precisely.

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition  # Kafka consumer types.
from aiokafka.errors import CommitFailedError, KafkaError  # Commit errors.

from txn_service.logging import get_logger  # Project logger factory.
from txn_service.messaging.schemas import TransactionEvent  # Parsed message schema (used for type hints).
from txn_service.worker.processor import TransactionProcessor  # Business logic invoked for each message.

# Module-level logger.
log = get_logger(__name__)
# Upper bound for the retry delay when infrastructure is down.
MAX_INFRA_BACKOFF_SECONDS = 30.0


# Generic helper (PEP 695 type parameter T): returns whatever the operation returns.
async def retry_forever[T](operation: Callable[[], Awaitable[T]], what: str) -> T:
    """Infrastructure failures (DB/broker down) must not drop messages: back off and retry."""
    # Initial wait before the first retry.
    delay = 0.5
    # Loop until the operation succeeds.
    while True:
        try:
            # Success: return the result and leave the loop.
            return await operation()
        except asyncio.CancelledError:
            # Never swallow cancellation, or shutdown would hang.
            raise
        except Exception as exc:
            # Log the failure with the current delay for diagnosis.
            log.error("infra_error_retrying", operation=what, error=repr(exc), retry_in=delay)
            # Wait before trying again.
            await asyncio.sleep(delay)
            # Exponential backoff, capped.
            delay = min(delay * 2, MAX_INFRA_BACKOFF_SECONDS)


# Handles one partition: ingests and completes its messages in order.
class PartitionWorker:
    def __init__(
        self,
        # Partition this worker owns.
        tp: TopicPartition,
        # Business logic to run on each message.
        processor: TransactionProcessor,
        # Callback that commits an offset for a partition.
        commit: Callable[[TopicPartition, int], Awaitable[None]],
    ) -> None:
        # Remember which partition this is.
        self.tp = tp
        self._processor = processor
        self._commit = commit
        # Messages fetched from Kafka and waiting for ingest.
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        # Ingested messages (record plus event to finish, or None if nothing left to do).
        self._pending: asyncio.Queue[tuple[Any, TransactionEvent | None]] = asyncio.Queue()
        # Start both stage loops; one task per stage keeps per-partition order.
        self._tasks = [
            asyncio.create_task(self._ingest_loop(), name=f"ingest-{tp.partition}"),
            asyncio.create_task(self._complete_loop(), name=f"complete-{tp.partition}"),
        ]

    # Exposed as an attribute-style read-only value.
    @property
    def backlog(self) -> int:
        # Total messages buffered in memory in both stages; drives backpressure.
        return self.inbox.qsize() + self._pending.qsize()

    async def _ingest_loop(self) -> None:
        # Run until cancelled by stop().
        while True:
            # Wait for the next message in partition order.
            record = await self.inbox.get()
            # Insert the row right away, retrying on DB/broker outages.
            event = await retry_forever(partial(self._processor.ingest, record.value, record.key), "ingest")
            # Hand over to the completion stage, preserving order.
            await self._pending.put((record, event))

    async def _complete_loop(self) -> None:
        # Run until cancelled by stop().
        while True:
            # Next ingested message, in order.
            record, event = await self._pending.get()
            # None means ingest decided there is nothing to process (invalid or duplicate).
            if event is not None:
                # Do the (slow) processing, retrying on infrastructure errors.
                await retry_forever(partial(self._processor.complete, event), "complete")
            # Commit offset+1 (the next message to read) only after full handling.
            await self._commit(self.tp, record.offset + 1)

    async def stop(self) -> None:
        # Cancel both stage tasks.
        for task in self._tasks:
            task.cancel()
        # Wait for them to finish; return_exceptions absorbs the CancelledErrors.
        await asyncio.gather(*self._tasks, return_exceptions=True)


# Owns the consumer: polls Kafka, dispatches to partition workers, applies backpressure.
class ConsumerRunner:
    def __init__(
        self,
        # Underlying Kafka consumer.
        consumer: AIOKafkaConsumer,
        # Business logic shared by all partition workers.
        processor: TransactionProcessor,
        # Topic to subscribe to.
        topic: str,
        *,
        # In-memory message cap per partition before fetching is paused.
        max_backlog_per_partition: int = 1000,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._topic = topic
        self._max_backlog = max_backlog_per_partition
        # One worker per currently assigned partition.
        self._workers: dict[TopicPartition, PartitionWorker] = {}
        # Partitions whose fetching is currently paused.
        self._paused: set[TopicPartition] = set()
        # Last time the poll loop ran; used by the health check.
        self.last_poll_monotonic = time.monotonic()

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        try:
            # Commit exactly this partition's offset.
            await self._consumer.commit({tp: offset})
        except (CommitFailedError, KafkaError) as exc:
            # Typically a rebalance: the new owner re-reads from the last commit; idempotency makes it safe.
            log.warning("commit_failed", partition=tp.partition, offset=offset, error=repr(exc))

    async def _drop_workers(self, partitions: set[TopicPartition]) -> None:
        for tp in partitions:
            # Remove the worker from the registry (None if there was none).
            worker = self._workers.pop(tp, None)
            # A dropped partition can no longer be paused.
            self._paused.discard(tp)
            if worker is not None:
                # Cancel its tasks.
                await worker.stop()

    def _worker_for(self, tp: TopicPartition) -> PartitionWorker:
        # Look up an existing worker.
        worker = self._workers.get(tp)
        if worker is None:
            # First message for this partition: create and register its worker.
            worker = self._workers[tp] = PartitionWorker(tp, self._processor, self._commit)
        return worker

    def _apply_backpressure(self) -> None:
        """Pause fetching for partitions with a large in-memory backlog; resume when drained."""
        for tp, worker in self._workers.items():
            # Backlog hit the cap: stop fetching more for this partition.
            if tp not in self._paused and worker.backlog >= self._max_backlog:
                self._consumer.pause(tp)
                self._paused.add(tp)
            # Drained to half the cap (hysteresis avoids flapping): resume fetching.
            elif tp in self._paused and worker.backlog <= self._max_backlog // 2:
                self._consumer.resume(tp)
                self._paused.discard(tp)

    async def run(self, stop: asyncio.Event) -> None:
        # Alias so the nested Listener class can reach this instance (its own `self` differs).
        runner = self

        # Callbacks Kafka invokes when partitions move between consumers in the group.
        class Listener(ConsumerRebalanceListener):  # type: ignore[misc]
            # Called before partitions are taken away from this consumer.
            async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
                log.info("partitions_revoked", partitions=sorted(tp.partition for tp in revoked))
                # Stop their workers so unfinished messages are redelivered to the new owner.
                await runner._drop_workers(set(revoked))

            # Called after new partitions are given to this consumer.
            async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
                # Workers are created lazily on the first record, so only log here.
                log.info("partitions_assigned", partitions=sorted(tp.partition for tp in assigned))

        # Join the consumer group for the topic, with rebalance callbacks.
        self._consumer.subscribe([self._topic], listener=Listener())
        log.info("consumer_started", topic=self._topic)
        try:
            # Poll until shutdown is requested.
            while not stop.is_set():
                # Fetch up to 1s worth of records, grouped by partition.
                batches = await self._consumer.getmany(timeout_ms=1000)
                # Record liveness for the health endpoint.
                self.last_poll_monotonic = time.monotonic()
                for tp, records in batches.items():
                    # Get (or create) the worker for this partition.
                    worker = self._worker_for(tp)
                    for record in records:
                        # Queue without blocking; backpressure bounds the queue size.
                        worker.inbox.put_nowait(record)
                # Pause or resume partitions based on current backlogs.
                self._apply_backpressure()
        finally:
            # Uncommitted in-flight messages are simply redelivered after restart.
            # Stop every worker (copy the keys since _drop_workers mutates the dict).
            await self._drop_workers(set(self._workers))
            log.info("consumer_stopped")
