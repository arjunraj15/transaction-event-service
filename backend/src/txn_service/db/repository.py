# Timestamp type.
from datetime import datetime

# SQL builders: select, raw text and update.
from sqlalchemy import func, select, text, update

# Postgres insert, which supports ON CONFLICT.
from sqlalchemy.dialects.postgresql import insert

# Async session types.
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# ORM model and status enum.
from txn_service.db.models import Transaction, TransactionStatus

# Event type consumed from Kafka.
from txn_service.messaging.schemas import TransactionEvent


# Data-access layer for transactions.
class TransactionRepository:
    # Takes a factory that creates database sessions.
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        # Keep the factory for use in each method.
        self._sessions = session_factory

    # Idempotent insert used by the Kafka consumer.
    async def insert_if_absent(self, event: TransactionEvent) -> bool:
        """Insert a PROCESSING row. Returns False if the transaction_id already exists (duplicate)."""
        # Build the INSERT statement.
        stmt = (
            # Insert into the transactions table.
            insert(Transaction)
            # Column values.
            .values(
                transaction_id=event.transaction_id,
                source_account=event.source_account,
                destination_account=event.destination_account,
                amount=event.amount,
                currency=event.currency,
                status=TransactionStatus.PROCESSING,
                created_at=event.received_at,
            )
            # Silently skip if the id already exists (duplicate delivery).
            .on_conflict_do_nothing(index_elements=[Transaction.transaction_id])
            # Return the id only when a row was actually inserted.
            .returning(Transaction.transaction_id)
        )
        # Open a session and commit when the block ends.
        async with self._sessions.begin() as session:
            # Run it; None means a conflict happened.
            inserted = (await session.execute(stmt)).scalar_one_or_none()
        # True if a new row was inserted.
        return inserted is not None

    # Look up just the status for a transaction.
    async def get_status(self, transaction_id: str) -> str | None:
        # Open a read-only session.
        async with self._sessions() as session:
            # Fetch a single value.
            status: str | None = await session.scalar(
                # Select the status column for this id.
                select(Transaction.status).where(Transaction.transaction_id == transaction_id)
            )
        # None if the transaction does not exist.
        return status

    # Database health check.
    async def ping(self) -> bool:
        # Any error means the database is not reachable.
        try:
            # Open a session.
            async with self._sessions() as session:
                # Run a trivial query.
                await session.execute(text("SELECT 1"))
        # Swallow every error: this is a health probe.
        except Exception:
            # Report unhealthy.
            return False
        # Report healthy.
        return True

    # List recent transactions.
    async def list_recent(
        self, limit: int, status: list[str] | None = None, offset: int = 0
    ) -> list[Transaction]:
        """Newest first, one page at a time; filters on any of the given statuses."""
        # Order by newest first (id breaks ties so pages are stable) and take one page.
        stmt = (
            select(Transaction)
            .order_by(Transaction.created_at.desc(), Transaction.transaction_id.desc())
            .limit(limit)
            .offset(offset)
        )
        # Apply the optional status filter.
        if status:
            # Narrow the query to the statuses.
            stmt = stmt.where(Transaction.status.in_(status))
        # Open a session.
        async with self._sessions() as session:
            # Run the query and return the rows as a list.
            return list((await session.scalars(stmt)).all())

    # Totals per status across the whole table (not just the recent page).
    async def count_by_status(self) -> dict[str, int]:
        # Group and count; served by the (status, created_at) index.
        stmt = select(Transaction.status, func.count()).group_by(Transaction.status)
        # Open a session.
        async with self._sessions() as session:
            # Map each status to its row count.
            return {str(s): int(n) for s, n in (await session.execute(stmt)).all()}

    # Fetch one transaction by primary key.
    async def get(self, transaction_id: str) -> Transaction | None:
        # Open a session.
        async with self._sessions() as session:
            # Primary-key lookup, None if missing.
            return await session.get(Transaction, transaction_id)

    # Move a transaction to PROCESSED.
    async def mark_processed(self, transaction_id: str, processed_at: datetime) -> bool:
        """Conditional transition: only a PROCESSING row can become PROCESSED, so it happens at most once."""
        # Build the UPDATE statement.
        stmt = (
            # Update the transactions table.
            update(Transaction)
            # Only rows matching all of these.
            .where(
                # The target transaction.
                Transaction.transaction_id == transaction_id,
                # Still PROCESSING, which makes the transition idempotent.
                Transaction.status == TransactionStatus.PROCESSING,
            )
            # Values to set.
            .values(
                # New status.
                status=TransactionStatus.PROCESSED,
                # Completion time.
                processed_at=processed_at,
                # Increment the attempt counter in SQL.
                attempts=Transaction.attempts + 1,
                # Clear any earlier error.
                last_error=None,
            )
        )
        # Open a session and commit at the end.
        async with self._sessions.begin() as session:
            # Run the update.
            result = await session.execute(stmt)
        # True if a row changed (rowcount is untyped, hence the ignore).
        return bool(result.rowcount)  # type: ignore[attr-defined]

    # Record a failed attempt without changing the status.
    async def record_attempt_error(self, transaction_id: str, error: str) -> None:
        # Build the UPDATE statement.
        stmt = (
            # Update the transactions table.
            update(Transaction)
            # Target the given transaction.
            .where(Transaction.transaction_id == transaction_id)
            # Increment attempts and store the error, truncated to 2000 chars.
            .values(attempts=Transaction.attempts + 1, last_error=error[:2000])
        )
        # Open a session and commit at the end.
        async with self._sessions.begin() as session:
            # Run the update.
            await session.execute(stmt)

    # Mark a transaction as permanently failed.
    async def mark_failed(self, transaction_id: str, error: str) -> None:
        # Build the UPDATE statement.
        stmt = (
            # Update the transactions table.
            update(Transaction)
            # Only rows matching all of these.
            .where(
                # The target transaction.
                Transaction.transaction_id == transaction_id,
                # Still PROCESSING, so finished rows are untouched.
                Transaction.status == TransactionStatus.PROCESSING,
            )
            # Set the status and the truncated error.
            .values(status=TransactionStatus.FAILED, last_error=error[:2000])
        )
        # Open a session and commit at the end.
        async with self._sessions.begin() as session:
            # Run the update.
            await session.execute(stmt)
