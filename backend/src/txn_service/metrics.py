from prometheus_client import Counter, Histogram  # Metric types.

# Request latency per HTTP call, labelled so it can be sliced by method, route and status code.
HTTP_REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route", "status"],
    # Bucket edges in seconds, dense around the 500ms webhook SLA.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
# Count of webhook requests, split by what happened to them.
WEBHOOKS_RECEIVED = Counter(
    "webhooks_received_total", "Webhooks received by outcome", ["outcome"]
)  # accepted | invalid_signature | broker_unavailable
# Count of Kafka messages the worker consumed, split by whether they were new.
TRANSACTIONS_INGESTED = Counter(
    "transactions_ingested_total", "Consumed events by result", ["result"]
)  # new | duplicate
# Count of transactions that finished successfully.
TRANSACTIONS_PROCESSED = Counter("transactions_processed_total", "Transactions moved to PROCESSED")
# Count of transactions (or invalid messages) sent to the dead-letter queue.
TRANSACTIONS_FAILED = Counter("transactions_failed_total", "Transactions sent to the DLQ")
# Count of individual failed attempts, including ones later retried successfully.
PROCESSING_ATTEMPT_ERRORS = Counter("processing_attempt_errors_total", "Failed processing attempts")
# Time from webhook receipt to completion.
END_TO_END_LATENCY = Histogram(
    "transaction_end_to_end_seconds",
    "Webhook received -> PROCESSED",
    # Seconds; buckets cluster around the expected ~30s simulated delay.
    buckets=(1, 5, 15, 30, 31, 35, 45, 60, 120, 300),
)
