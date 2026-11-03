import logging  # Standard library logging, used for levels and for capturing third-party logs.
import sys  # Used to write logs to stdout.

import structlog  # Structured logging library.


def configure_logging(level: str = "INFO") -> None:
    """JSON logs to stdout; bind `transaction_id` via structlog.contextvars for correlation."""
    # Processors shared by structlog and stdlib logs so both get identical fields.
    shared: list[structlog.types.Processor] = [
        # Add any context-bound values (e.g. transaction_id) to every entry.
        structlog.contextvars.merge_contextvars,
        # Add a "level" field.
        structlog.processors.add_log_level,
        # Add an ISO-8601 UTC timestamp.
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    # Set global structlog behaviour.
    structlog.configure(
        # Processing pipeline applied to each log call, in order.
        processors=[
            *shared,
            # Render stack_info if requested.
            structlog.processors.StackInfoRenderer(),
            # Turn exc_info into a formatted traceback string.
            structlog.processors.format_exc_info,
            # Final step: serialise the event dict to JSON.
            structlog.processors.JSONRenderer(),
        ],
        # Drop messages below the configured level cheaply, before any processing.
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        # Write straight to stdout (what container platforms collect).
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        # Cache logger objects for speed after first use.
        cache_logger_on_first_use=True,
    )
    # Route stdlib logs (uvicorn, aiokafka, sqlalchemy) through the same JSON format.
    # Handler that writes stdlib log records to stdout.
    handler = logging.StreamHandler(sys.stdout)
    # Format those records via structlog so they come out as JSON too.
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(processors=[*shared, structlog.processors.JSONRenderer()])
    )
    # The root logger receives records from all libraries.
    root = logging.getLogger()
    # Replace any existing handlers so logs are not duplicated or left in a different format.
    root.handlers = [handler]
    # Apply the configured level globally.
    root.setLevel(level.upper())
    # aiokafka is chatty at INFO; only show warnings and above.
    logging.getLogger("aiokafka").setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    # Fetch a logger by name (typically the module's __name__); annotated for type checkers.
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    # Hand it back to the caller.
    return logger
