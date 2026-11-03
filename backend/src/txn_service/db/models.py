# Base for the status enum.
import enum

# Timestamp type.
from datetime import datetime

# Exact decimal type for money.
from decimal import Decimal

# SQLAlchemy column types, constraints and SQL functions.
from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String, Text, func

# Declarative ORM base and typed column helpers.
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# Allowed lifecycle states of a transaction.
class TransactionStatus(enum.StrEnum):
    # Received and waiting to be processed.
    PROCESSING = "PROCESSING"
    # Processing completed.
    PROCESSED = "PROCESSED"
    # Processing gave up.
    FAILED = "FAILED"


# Shared declarative base for all models.
class Base(DeclarativeBase):
    # No extra members needed.
    pass


# ORM model for a processed transaction.
class Transaction(Base):
    # Table name in the database.
    __tablename__ = "transactions"

    # The primary key is the idempotency guarantee: one row per transaction_id, ever.
    transaction_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    # Account sending funds.
    source_account: Mapped[str] = mapped_column(String(128))
    # Account receiving funds.
    destination_account: Mapped[str] = mapped_column(String(128))
    # Exact decimal, 18 digits with 4 after the point.
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    # Three-letter currency code.
    currency: Mapped[str] = mapped_column(String(3))
    # Lifecycle state, defaults to PROCESSING.
    status: Mapped[str] = mapped_column(String(16), default=TransactionStatus.PROCESSING)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # webhook received_at
    # Completion time; NULL until processed.
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Number of processing attempts so far.
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    # Most recent error text, if any.
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Last-modified time (timezone-aware): DB default now(), refreshed on every update.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Table-level constraints and indexes.
    __table_args__ = (
        # Database-level guard against invalid statuses.
        CheckConstraint("status IN ('PROCESSING', 'PROCESSED', 'FAILED')", name="ck_transactions_status"),
        # Index for listing by status, newest first.
        Index("ix_transactions_status_created_at", "status", "created_at"),
    )
