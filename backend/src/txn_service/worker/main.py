"""Consumer service entrypoint: `python -m txn_service.worker.main`.

Also serves /healthz and /metrics on WORKER_HTTP_PORT (or PORT, if the platform sets one).
"""

import asyncio  # Tasks, events and the event loop.
import contextlib  # contextmanager helper and suppress().
import os  # Read the PORT environment variable.
import signal  # Signal names for graceful shutdown.
import time  # Monotonic clock for health-check age.
from collections.abc import Iterator  # Type for the generator-based context manager below.

import uvicorn  # ASGI server used to serve the health/metrics app.
from fastapi import FastAPI, Response, status  # Health app web framework.
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # Prometheus text output.

from txn_service.config import get_settings  # Cached application settings.
from txn_service.db.engine import (  # Database engine and session factory builders.
    create_engine,
    create_session_factory,
)
from txn_service.db.repository import TransactionRepository  # Database access layer for transactions.
from txn_service.events import PgNotifyEventSink  # Dashboard event sink.
from txn_service.logging import configure_logging, get_logger  # Logging setup and logger factory.
from txn_service.messaging.kafka import (  # Kafka helpers.
    KafkaDeadLetterSink,
    create_consumer,
    create_producer,
)
from txn_service.worker.consumer import ConsumerRunner  # Polling loop and partition handling.
from txn_service.worker.processor import TransactionProcessor  # Per-message business logic.

# Module-level logger.
log = get_logger(__name__)
# If the consumer has not polled for this long, report unhealthy.
POLL_STALE_AFTER_SECONDS = 60


# Builds a minimal HTTP app exposing liveness and metrics for the platform and Prometheus.
def build_health_app(runner: ConsumerRunner) -> FastAPI:
    # Disable the interactive docs and schema endpoints; this app is internal.
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    # Liveness probe endpoint.
    @app.get("/healthz")
    async def healthz(response: Response) -> dict[str, object]:
        # Seconds since the consumer loop last polled Kafka.
        age = time.monotonic() - runner.last_poll_monotonic
        # A stuck consumer stops polling, so staleness signals failure.
        healthy = age < POLL_STALE_AFTER_SECONDS
        if not healthy:
            # Return 503 so the platform restarts the worker.
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        # Body gives a human-readable status and the measured age.
        return {"status": "HEALTHY" if healthy else "UNHEALTHY", "seconds_since_last_poll": round(age, 1)}

    # Prometheus scrape endpoint.
    @app.get("/metrics")
    async def metrics() -> Response:
        # Serialise all registered metrics in the Prometheus text format.
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # Hand the configured app to the caller.
    return app


class _HealthServer(uvicorn.Server):
    """uvicorn would hijack SIGTERM/SIGINT; the worker owns signal handling for a coordinated shutdown."""

    # Override uvicorn's signal-capturing hook with a no-op.
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # Do nothing around the body, so uvicorn installs no signal handlers.
        yield


# Make SIGTERM/SIGINT set the stop event so shutdown can be coordinated.
def _install_signal_handlers(stop: asyncio.Event) -> None:
    # The loop must already be running (we are called from async code).
    loop = asyncio.get_running_loop()
    # SIGTERM comes from the platform, SIGINT from Ctrl+C.
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            # Preferred, loop-integrated handler; sets the event when the signal arrives.
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows dev machines
            # Fallback: plain handler that safely schedules stop.set on the loop thread.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


# Main async routine: wire everything up, run, then shut down cleanly.
async def run() -> None:
    # Load configuration.
    settings = get_settings()
    # Set up structured logging at the configured level.
    configure_logging(settings.log_level)
    # Event set when shutdown is requested.
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    # Main DB engine used for processing.
    engine = create_engine(settings)
    # Separate 1-connection engine for dashboard events, so they never take processing connections.
    events_engine = create_engine(settings.model_copy(update={"db_pool_size": 1, "db_max_overflow": 0}))
    # Publisher of live events to the dashboard.
    events = PgNotifyEventSink(events_engine, settings.events_channel)
    producer = create_producer(settings)  # used for the DLQ
    # Kafka consumer for incoming transactions.
    consumer = create_consumer(settings)
    # Connect to the brokers.
    await producer.start()
    await consumer.start()

    # Business logic wired with its dependencies.
    processor = TransactionProcessor(
        # Database access built on the main engine.
        TransactionRepository(create_session_factory(engine)),
        # Where unprocessable messages are sent.
        KafkaDeadLetterSink(producer, settings.kafka_topic_dlq),
        settings,
        events=events,
    )
    # Polling loop bound to the transactions topic.
    runner = ConsumerRunner(consumer, processor, settings.kafka_topic_transactions)

    # Platforms like Render provide PORT; otherwise use the configured worker port.
    port = int(os.environ.get("PORT", settings.worker_http_port))
    # Health/metrics server that leaves signal handling to us.
    http = _HealthServer(
        uvicorn.Config(build_health_app(runner), host="0.0.0.0", port=port, log_config=None)  # noqa: S104
    )
    # Serve HTTP in the background.
    http_task = asyncio.create_task(http.serve())

    # Run the Kafka consume loop in the background.
    consume_task = asyncio.create_task(runner.run(stop))
    # Task that completes when a shutdown signal arrives.
    stop_task = asyncio.create_task(stop.wait())
    try:
        # Wait for either a shutdown signal or the consumer ending on its own.
        done, _ = await asyncio.wait({consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if consume_task in done:
            consume_task.result()  # surface a crash so the platform restarts us
    finally:
        log.info("worker_shutting_down")
        # Make sure the consume loop's exit condition is set.
        stop.set()
        # Cancel it in case it is mid-poll.
        consume_task.cancel()
        # Wait for it to finish; ignore the expected CancelledError.
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        # Ask uvicorn to exit, then wait for it.
        http.should_exit = True
        await http_task
        await consumer.stop()  # leaves the group promptly so partitions rebalance fast
        # Flush and close the DLQ producer.
        await producer.stop()
        # Flush pending dashboard events.
        await events.close()
        # Close database connection pools.
        await events_engine.dispose()
        await engine.dispose()
        # Clean up the unused signal-waiter task.
        stop_task.cancel()
        log.info("worker_stopped")


# Synchronous entry point.
def main() -> None:
    # Start the event loop and run the worker until it finishes.
    asyncio.run(run())


# Only run when executed as a script/module, not when imported.
if __name__ == "__main__":
    main()
