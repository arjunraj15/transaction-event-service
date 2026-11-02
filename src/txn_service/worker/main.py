"""Consumer service entrypoint: `python -m txn_service.worker.main`.

Also serves /healthz and /metrics on WORKER_HTTP_PORT (or PORT, if the platform sets one).
"""

import asyncio
import contextlib
import os
import signal
import time
from collections.abc import Iterator

import uvicorn
from fastapi import FastAPI, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from txn_service.config import get_settings
from txn_service.db.engine import create_engine, create_session_factory
from txn_service.db.repository import TransactionRepository
from txn_service.logging import configure_logging, get_logger
from txn_service.messaging.kafka import KafkaDeadLetterSink, create_consumer, create_producer
from txn_service.worker.consumer import ConsumerRunner
from txn_service.worker.processor import TransactionProcessor

log = get_logger(__name__)
POLL_STALE_AFTER_SECONDS = 60


def build_health_app(runner: ConsumerRunner) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/healthz")
    async def healthz(response: Response) -> dict[str, object]:
        age = time.monotonic() - runner.last_poll_monotonic
        healthy = age < POLL_STALE_AFTER_SECONDS
        if not healthy:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "HEALTHY" if healthy else "UNHEALTHY", "seconds_since_last_poll": round(age, 1)}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


class _HealthServer(uvicorn.Server):
    """uvicorn would hijack SIGTERM/SIGINT; the worker owns signal handling for a coordinated shutdown."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows dev machines
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    engine = create_engine(settings)
    producer = create_producer(settings)  # used for the DLQ
    consumer = create_consumer(settings)
    await producer.start()
    await consumer.start()

    processor = TransactionProcessor(
        TransactionRepository(create_session_factory(engine)),
        KafkaDeadLetterSink(producer, settings.kafka_topic_dlq),
        settings,
    )
    runner = ConsumerRunner(consumer, processor, settings.kafka_topic_transactions)

    port = int(os.environ.get("PORT", settings.worker_http_port))
    http = _HealthServer(
        uvicorn.Config(build_health_app(runner), host="0.0.0.0", port=port, log_config=None)  # noqa: S104
    )
    http_task = asyncio.create_task(http.serve())

    consume_task = asyncio.create_task(runner.run(stop))
    stop_task = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if consume_task in done:
            consume_task.result()  # surface a crash so the platform restarts us
    finally:
        log.info("worker_shutting_down")
        stop.set()
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        http.should_exit = True
        await http_task
        await consumer.stop()  # leaves the group promptly so partitions rebalance fast
        await producer.stop()
        await engine.dispose()
        stop_task.cancel()
        log.info("worker_stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
