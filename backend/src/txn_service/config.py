from functools import lru_cache
from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "local"
    log_level: str = "INFO"

    # Accepts the raw URL from Neon/Render (postgres://...?sslmode=require); normalized in db.engine.
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/transactions"
    db_pool_size: int = 5
    db_max_overflow: int = 5

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: Literal["PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_api_key: str | None = None
    kafka_api_secret: SecretStr | None = None
    kafka_topic_transactions: str = "transactions.received"
    kafka_topic_dlq: str = "transactions.dlq"
    kafka_consumer_group: str = "transaction-processor"
    kafka_topic_partitions: int = 6
    kafka_topic_replication_factor: int = 1  # Confluent Cloud requires 3

    # The API waits at most this long for the broker ack so the webhook stays under the 500ms SLA.
    produce_timeout_seconds: float = 0.4

    # Simulated external processing time measured from when the webhook was received.
    processing_delay_seconds: float = 30.0
    processing_max_attempts: int = 3
    processing_retry_backoff_seconds: float = 1.0

    # HMAC-SHA256 webhook signature verification is enabled only when a secret is configured.
    webhook_secret: SecretStr | None = None
    webhook_signature_header: str = "X-Razorpay-Signature"

    worker_http_port: int = 8081

    # Demo dashboard (a separate static site): Postgres LISTEN/NOTIFY channel for its live events.
    events_channel: str = "txn_events"
    # Required in the X-Admin-Key header for the dashboard's admin endpoints when set (always in prod).
    admin_api_key: SecretStr | None = None
    # Comma-separated browser origins allowed to call the API, e.g. the dashboard's URL. A bare host
    # (as Render's `fromService` provides) is treated as https://<host>.
    cors_allowed_origins: str = ""

    @property
    def cors_origins(self) -> list[str]:
        origins = [o.strip().rstrip("/") for o in self.cors_allowed_origins.split(",") if o.strip()]
        return [o if "://" in o else f"https://{o}" for o in origins]

    @field_validator("kafka_api_key", "kafka_api_secret", "webhook_secret", "admin_api_key", mode="before")
    @classmethod
    def _blank_means_unset(cls, value: object) -> object:
        # Platforms (e.g. Render) set optional variables left blank to "", which must not count as configured.
        if isinstance(value, str) and not value.strip():
            return None
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
