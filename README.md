# Transaction Event Service

A service that receives transaction webhooks from payment processors (e.g. RazorPay), **acknowledges them in milliseconds**, and **processes them reliably in the background**. Repeated webhooks for the same transaction are processed **exactly once**.

**Live API:** `https://<your-api>.up.railway.app` · interactive docs at `/docs`

**Stack:** Python 3.12 · FastAPI · Kafka (Confluent Cloud) · PostgreSQL (Neon) · SQLAlchemy 2 + Alembic · Docker · Railway · GitHub Actions

---

## Architecture

```
Payment processor ──HTTPS──▶ API service (FastAPI)
                               1. verify HMAC signature (optional)
                               2. validate payload (exact Decimal money, ISO-4217 currency)
                               3. publish to Kafka: key=transaction_id, acks=all, idempotent producer
                               4. 202 Accepted   (no processing on the request path)
                                     │
                     Kafka topic  transactions.received  (6 partitions)
                                     │
                               Consumer service (separate process)
                               per partition, in order:
                                 ingest   → INSERT … ON CONFLICT DO NOTHING  (row = PROCESSING)
                                 complete → wait until received_at + 30s → UPDATE → PROCESSED
                                 commit offset only after the DB commit
                               failures → retries with backoff → transactions.dlq
                                     │
                               PostgreSQL (source of truth) ◀── GET /v1/transactions/{id}
```

| Requirement | How it is met |
|---|---|
| **202 within 500 ms** | The request path does only validation and one Kafka publish. The broker ack is awaited with a 400 ms timeout (`PRODUCE_TIMEOUT_SECONDS`), and nothing is processed inline. |
| **30 s background processing** | The consumer waits until `received_at + 30s`, then marks the row `PROCESSED`. The delay is measured from webhook receipt, so consumer lag doesn't add to it. |
| **Idempotency** | `transaction_id` is the table's **primary key**. The insert is `ON CONFLICT DO NOTHING`, and the final transition is `UPDATE … WHERE status='PROCESSING'`, so it can happen only once. Duplicates share a Kafka partition key and are therefore handled one after another. |
| **Persistent storage** | PostgreSQL, with `NUMERIC(18,4)` amounts, `TIMESTAMPTZ` times, status, attempts and last error. |
| **Never lose a transaction** | A 202 is returned only after Kafka has durably stored the event (`acks=all`). Offsets are committed only after processing, so a crash leads to redelivery, which is harmless because processing is idempotent. |

## API

| Method & path | Description | Responses |
|---|---|---|
| `POST /v1/webhooks/transactions` | Receive a transaction webhook | `202` accepted · `401` bad signature · `422` invalid body · `503` broker unavailable, retry |
| `GET /` | Liveness / health check | `200 {"status":"HEALTHY","current_time":"…Z"}` |
| `GET /v1/transactions/{transaction_id}` | Transaction status (for testing) | `200 [ {...} ]` · `404` |
| `GET /health/ready` | Readiness: DB + Kafka reachable | `200` / `503` |
| `GET /metrics` | Prometheus metrics | `200` |
| `GET /docs` | OpenAPI / Swagger UI | `200` |

```bash
curl -i -X POST "$BASE_URL/v1/webhooks/transactions" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
# HTTP/1.1 202 Accepted
# {"status":"accepted","transaction_id":"txn_abc123def456"}

curl "$BASE_URL/v1/transactions/txn_abc123def456"
# [{"transaction_id":"txn_abc123def456", ..., "status":"PROCESSING","created_at":"…","processed_at":null}]
# ~30 s later: "status":"PROCESSED","processed_at":"…"
```

## Testing the success criteria

| Scenario | How to check it by hand | Automated |
|---|---|---|
| 1. Single transaction | POST once, then GET: `PROCESSING` → `PROCESSED` after ~30 s | `test_single_transaction_processed_after_delay` |
| 2. Duplicate prevention | POST the same body 5–10 times. There's one row, `attempts = 1`, and `transactions_processed_total` rises by exactly 1 | `test_duplicate_webhooks_processed_exactly_once` |
| 3. Performance | `k6 run -e BASE_URL=$BASE_URL loadtest/webhooks.k6.js` (200 VUs; fails unless p95 < 500 ms and 100% of responses are 202) | k6 thresholds |
| 4. Reliability | Restart the worker during the 30 s window. The transaction still ends up `PROCESSED`, once | `test_worker_crash_mid_delay_does_not_lose_transaction`, `test_poison_message_goes_to_dlq` |

```bash
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" -X POST "$BASE_URL/v1/webhooks/transactions" \
    -H "Content-Type: application/json" \
    -d '{"transaction_id":"txn_dup_1","source_account":"a","destination_account":"b","amount":10,"currency":"INR"}'
done
```

## Running locally

Prerequisites: Docker, and [uv](https://docs.astral.sh/uv/) for development.

```bash
docker compose up --build
```

This starts Postgres, Kafka (KRaft), Kafka UI (http://localhost:8080), a one-shot `init` (creates the topics and runs the migrations), the **API** (http://localhost:8000/docs) and the **worker** (http://localhost:8081/healthz). For a quick demo, use `PROCESSING_DELAY_SECONDS=5 docker compose up`.

Development without containers for the app itself: start only `postgres` and `kafka` with compose, then

```bash
uv sync
cp .env.example .env
uv run python scripts/create_topics.py && uv run alembic upgrade head
uv run uvicorn txn_service.api.main:app --reload          # terminal 1
uv run python -m txn_service.worker.main                   # terminal 2
```

### Tests and quality gates

```bash
uv run pytest tests/unit          # fast, no infrastructure needed (fakes + deterministic clock)
uv run pytest tests/integration   # real Postgres + Kafka via Testcontainers (needs Docker; skipped otherwise)
uv run ruff check . && uv run ruff format --check . && uv run mypy   # lint, format, strict typing
```

CI (GitHub Actions) runs lint, type checks, unit and integration tests, and a Docker build on every push and PR.

## Deployment (Confluent Cloud + Neon + Railway)

1. **Kafka (Confluent Cloud).** Create a *Basic* cluster and an API key, then create the topics once:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=<pkc-…:9092> KAFKA_SECURITY_PROTOCOL=SASL_SSL \
   KAFKA_API_KEY=<key> KAFKA_API_SECRET=<secret> KAFKA_TOPIC_REPLICATION_FACTOR=3 \
   uv run python scripts/create_topics.py
   ```
2. **PostgreSQL (Neon).** Create a project and copy its connection string. The `postgresql://…?sslmode=require` form can be used as-is.
3. **Railway.** Create two services from this GitHub repo:
   - **api**: config file `deploy/railway/api.json`. This runs migrations as a pre-deploy step, health-checks `/health/ready`, and needs a public domain.
   - **worker**: config file `deploy/railway/worker.json`. Start command `python -m txn_service.worker.main`, health check `/healthz`.
   - Set the same variables on both services (shared variables work well):

   | Variable | Value |
   |---|---|
   | `DATABASE_URL` | Neon connection string |
   | `KAFKA_BOOTSTRAP_SERVERS` | Confluent bootstrap server |
   | `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` |
   | `KAFKA_API_KEY` / `KAFKA_API_SECRET` | Confluent API key and secret |
   | `KAFKA_TOPIC_REPLICATION_FACTOR` | `3` |
   | `WEBHOOK_SECRET` | *(optional)* enables signature verification |
   | `ENVIRONMENT` | `production` |

4. Smoke test: `curl https://<api-domain>/health/ready`, then run the scenarios above.

## Technical choices

**FastAPI.** Async I/O keeps the webhook path non-blocking. Pydantic v2 validation and the generated OpenAPI docs come with it.

**Kafka as the ingestion buffer.** Webhook traffic is bursty and processing is slow (30 s), so receiving and processing are separated by a durable, replayable log:
- The API can return 202 as soon as the event is safely stored.
- Consumers scale horizontally up to the partition count.
- Partitioning by `transaction_id` keeps events for one transaction ordered on a single consumer.
- Events can be replayed for recovery or backfills, and failures are isolated in a DLQ.

The API writes only to Kafka. Writing to both the DB and Kafka on the request path would create a dual-write consistency problem.

**Postgres as the source of truth and the idempotency guard.** Kafka delivers at-least-once, and its idempotent producer only removes duplicates caused by producer retries, not repeat webhooks. Exactly-once *effects* come from the database: a primary key, `ON CONFLICT DO NOTHING`, and a conditional state transition. Delivering at least once with idempotent processing gives effectively exactly-once results.

**Consumer design.** Each partition gets two ordered stages:
- **Ingest** writes the row the moment the event is consumed, so `GET` shows `PROCESSING` straight away.
- **Complete** waits out the delay, finalizes the row, then commits the offset.

Every event has the same delay, so events in a partition come due in the order they arrived, and nothing gets stuck behind a slower event. Other details:
- Backpressure pauses a partition when its in-memory backlog grows too large.
- Rebalances drop in-flight work without committing it, so the next owner safely redoes it.
- Infrastructure errors (DB or broker down) are retried indefinitely with backoff and never dead-lettered.
- Business failures get 3 attempts with exponential backoff, then the event goes to `transactions.dlq` with the reason in a header, and the row is marked `FAILED`.

**Money handling.** Amounts are `Decimal` end to end: `NUMERIC(18,4)` in the database and a string inside Kafka messages, never a float. The query response sends a JSON number to match the published contract. *Note:* the brief's example (`150.50`) suggests floats, which a real payments system should not use.

**Security.** Optional RazorPay-style HMAC-SHA256 verification of the raw body, using a constant-time comparison. Secrets come only from the environment. The container runs as a non-root user.

**Observability.**
- Structured JSON logs (structlog), with `transaction_id` attached to every log line.
- Prometheus metrics: request latency, webhook outcomes, ingested new vs. duplicate, processed, failed, attempt errors, and end-to-end latency.
- Separate liveness and readiness endpoints, plus a health check on the worker based on its last poll time.

**Operations.** Alembic migrations run as a pre-deploy step rather than `create_all` at startup. One image serves both processes. Shutdown on SIGTERM is graceful: the producer is flushed and the consumer leaves the group promptly.

### Trade-offs and known limitations

- **Read-after-write gap.** For roughly a second after the 202, `GET` can return 404, until the consumer has ingested the event. This is the cost of a single durable write on the request path.
- **Publish timeout.** If the broker ack arrives after the 400 ms budget, the API returns 503 even though the event may already be stored. The processor's retry then becomes a duplicate event, which the idempotent consumer absorbs.
- **Side effects of a real downstream call.** A crash after the external call but before the DB update means the call is repeated (at-least-once). A real integration would pass `transaction_id` as the idempotency key. The `external_call` hook in `worker/processor.py` is where it would go.

### Production next steps (out of scope here)

OpenTelemetry tracing across API → Kafka → worker · Sentry · Schema Registry with Avro/Protobuf event contracts · autoscaling consumers on lag (KEDA) · a DLQ replay tool and alerting · Terraform for infrastructure · rate limiting and processor IP allow-lists.

## Configuration

All settings are environment variables (see `.env.example` and `src/txn_service/config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | local Postgres | Any `postgres://`, `postgresql://` or `postgresql+asyncpg://` URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | `SASL_SSL` for Confluent Cloud |
| `KAFKA_API_KEY` / `KAFKA_API_SECRET` | – | SASL/PLAIN credentials |
| `KAFKA_TOPIC_TRANSACTIONS` / `KAFKA_TOPIC_DLQ` | `transactions.received` / `transactions.dlq` | Topics |
| `PRODUCE_TIMEOUT_SECONDS` | `0.4` | Broker-ack budget inside the 500 ms SLA |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external processing time |
| `PROCESSING_MAX_ATTEMPTS` | `3` | Attempts before dead-lettering |
| `WEBHOOK_SECRET` | – | Enables HMAC signature verification |

## Project layout

```
src/txn_service/
  api/            FastAPI app factory, routes (webhooks, transactions, health), HMAC check
  worker/         consumer runtime (partitions, offsets, backpressure) + processor (business logic)
  messaging/      event schemas, Kafka producer/consumer/DLQ
  db/             SQLAlchemy model, repository (idempotent writes), engine
  config.py  logging.py  metrics.py
alembic/          migrations
tests/unit        API, schemas, processor, consumer (no infrastructure)
tests/integration end-to-end with real Postgres + Kafka (Testcontainers)
loadtest/         k6 script
deploy/railway/   per-service Railway config
```
