import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI, Request, Response

from txn_service.api.dependencies import Publisher, TransactionReader
from txn_service.api.routes import health, transactions, webhooks
from txn_service.config import Settings, get_settings
from txn_service.db.engine import create_engine, create_session_factory
from txn_service.db.repository import TransactionRepository
from txn_service.logging import configure_logging, get_logger
from txn_service.messaging.kafka import TransactionPublisher, create_producer
from txn_service.metrics import HTTP_REQUEST_DURATION

log = get_logger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    publisher: Publisher | None = None,
    reader: TransactionReader | None = None,
) -> FastAPI:
    """App factory. Tests inject fakes for `publisher`/`reader`; production wires Kafka and Postgres."""
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            app.state.settings = settings
            if reader is None:
                engine = create_engine(settings)
                stack.push_async_callback(engine.dispose)
                app.state.reader = TransactionRepository(create_session_factory(engine))
            else:
                app.state.reader = reader
            if publisher is None:
                producer = create_producer(settings)
                await producer.start()
                stack.push_async_callback(producer.stop)  # flushes in-flight messages on shutdown
                app.state.publisher = TransactionPublisher(producer, settings)
            else:
                app.state.publisher = publisher
            log.info("api_started", environment=settings.environment)
            yield
            log.info("api_stopping")

    app = FastAPI(
        title="Transaction Event Service",
        version="1.0.0",
        description="Receives payment-processor webhooks and processes transactions reliably via Kafka.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def record_latency(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        HTTP_REQUEST_DURATION.labels(
            method=request.method,
            route=getattr(route, "path", "unmatched"),  # template path keeps label cardinality bounded
            status=str(response.status_code),
        ).observe(time.perf_counter() - start)
        return response

    app.include_router(health.router)
    app.include_router(webhooks.router)
    app.include_router(transactions.router)
    return app
