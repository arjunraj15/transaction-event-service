from functools import lru_cache  # Build-once cache.
from typing import Literal  # Literal restricts a field to a fixed set of allowed string values.

from pydantic import SecretStr, field_validator  # Secret masking, validator.
from pydantic_settings import BaseSettings, SettingsConfigDict  # Env-backed settings.


# Typed application configuration, populated from env vars (names match fields, case-insensitive).
class Settings(BaseSettings):
    # Also read a local .env file, and ignore unknown variables instead of raising.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Deployment name (local/staging/prod); used to switch environment-specific behaviour.
    environment: str = "local"
    # Minimum severity emitted by the logger.
    log_level: str = "INFO"

    # Accepts the raw URL from Neon/Render (postgres://...?sslmode=require); normalized in db.engine.
    # Default points at a local dev Postgres using the asyncpg driver.
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/transactions"
    # Number of persistent connections kept open in the SQLAlchemy pool.
    db_pool_size: int = 5
    # Extra temporary connections allowed beyond the pool size under load.
    db_max_overflow: int = 5

    # Comma-separated broker addresses used for the initial cluster connection.
    kafka_bootstrap_servers: str = "localhost:9092"
    # Plain locally; SASL over TLS for managed clusters such as Confluent Cloud.
    kafka_security_protocol: Literal["PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    # SASL authentication mechanism (only used with SASL_SSL).
    kafka_sasl_mechanism: str = "PLAIN"
    # SASL username; None means no credentials configured.
    kafka_api_key: str | None = None
    # SASL password, wrapped so it never shows up in logs.
    kafka_api_secret: SecretStr | None = None
    # Topic the webhook API publishes incoming transactions to.
    kafka_topic_transactions: str = "transactions.received"
    # Dead-letter topic for messages that cannot be processed.
    kafka_topic_dlq: str = "transactions.dlq"
    # Consumer group id; workers sharing it split the topic's partitions between them.
    kafka_consumer_group: str = "transaction-processor"
    # Partition count when creating topics (caps consumer parallelism).
    kafka_topic_partitions: int = 6
    # Copies of each partition kept across brokers when creating topics.
    kafka_topic_replication_factor: int = 1  # Confluent Cloud requires 3

    # The API waits at most this long for the broker ack so the webhook stays under the 500ms SLA.
    produce_timeout_seconds: float = 0.4

    # Simulated external processing time measured from when the webhook was received.
    processing_delay_seconds: float = 30.0
    # How many times a failing external call is tried before dead-lettering.
    processing_max_attempts: int = 3
    # Base wait between attempts; doubled after each failure (exponential backoff).
    processing_retry_backoff_seconds: float = 1.0

    # HMAC-SHA256 webhook signature verification is enabled only when a secret is configured.
    webhook_secret: SecretStr | None = None
    # Request header that carries the webhook signature.
    webhook_signature_header: str = "X-Razorpay-Signature"

    # Port for the worker's health/metrics HTTP server.
    worker_http_port: int = 8081

    # Demo dashboard (a separate static site): Postgres LISTEN/NOTIFY channel for its live events.
    events_channel: str = "txn_events"
    # Required in the X-Admin-Key header for the dashboard's admin endpoints when set (always in prod).
    admin_api_key: SecretStr | None = None
    # Comma-separated browser origins allowed to call the API, e.g. the dashboard's URL. A bare host
    # is treated as https://<host>.
    cors_allowed_origins: str = ""

    # Computed attribute (not an env var) exposing the CORS origins as a clean list.
    @property
    def cors_origins(self) -> list[str]:
        # Split on commas, trim whitespace and trailing slashes, and skip empty entries.
        origins = [o.strip().rstrip("/") for o in self.cors_allowed_origins.split(",") if o.strip()]
        # Prefix bare hosts (no scheme) with https://.
        return [o if "://" in o else f"https://{o}" for o in origins]

    # Run on these fields before type validation ("before") so blank values can be normalised.
    @field_validator("kafka_api_key", "kafka_api_secret", "webhook_secret", "admin_api_key", mode="before")
    # Pydantic validators on the class itself must be classmethods.
    @classmethod
    def _blank_means_unset(cls, value: object) -> object:
        # Platforms (e.g. Render) set optional variables left blank to "", which must not count as configured.
        # Treat empty or whitespace-only strings as "not set".
        if isinstance(value, str) and not value.strip():
            # None makes the optional field behave as unconfigured.
            return None
        # Any real value passes through unchanged.
        return value


# Memoise so every caller shares one Settings instance and .env is parsed once.
@lru_cache
def get_settings() -> Settings:
    # Build (on first call) and return the settings object.
    return Settings()
