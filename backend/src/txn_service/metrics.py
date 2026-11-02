from prometheus_client import Counter, Histogram

HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route", "status"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
WEBHOOKS_RECEIVED = Counter(
    "webhooks_received_total", "Webhooks received by outcome", ["outcome"]
)  # accepted | invalid_signature | broker_unavailable
TRANSACTIONS_INGESTED = Counter(
    "transactions_ingested_total", "Consumed events by result", ["result"]
)  # new | duplicate
TRANSACTIONS_PROCESSED = Counter("transactions_processed_total", "Transactions moved to PROCESSED")
TRANSACTIONS_FAILED = Counter("transactions_failed_total", "Transactions sent to the DLQ")
PROCESSING_ATTEMPT_ERRORS = Counter("processing_attempt_errors_total", "Failed processing attempts")
END_TO_END_LATENCY = Histogram(
    "transaction_end_to_end_seconds",
    "Webhook received -> PROCESSED",
    buckets=(1, 5, 15, 30, 31, 35, 45, 60, 120, 300),
)
