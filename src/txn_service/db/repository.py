from datetime import datetime

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from txn_service.db.models import Transaction, TransactionStatus
from txn_service.messaging.schemas import TransactionEvent


class TransactionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = session_factory

    async def insert_if_absent(self, event: TransactionEvent) -> bool:
        """Insert a PROCESSING row. Returns False if the transaction_id already exists (duplicate)."""
        stmt = (
            insert(Transaction)
            .values(
                transaction_id=event.transaction_id,
                source_account=event.source_account,
                destination_account=event.destination_account,
                amount=event.amount,
                currency=event.currency,
                status=TransactionStatus.PROCESSING,
                created_at=event.received_at,
            )
            .on_conflict_do_nothing(index_elements=[Transaction.transaction_id])
            .returning(Transaction.transaction_id)
        )
        async with self._sessions.begin() as session:
            inserted = (await session.execute(stmt)).scalar_one_or_none()
        return inserted is not None

    async def get_status(self, transaction_id: str) -> str | None:
        async with self._sessions() as session:
            status: str | None = await session.scalar(
                select(Transaction.status).where(Transaction.transaction_id == transaction_id)
            )
        return status

    async def ping(self) -> bool:
        try:
            async with self._sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return False
        return True

    async def get(self, transaction_id: str) -> Transaction | None:
        async with self._sessions() as session:
            return await session.get(Transaction, transaction_id)

    async def mark_processed(self, transaction_id: str, processed_at: datetime) -> bool:
        """Conditional transition: only a PROCESSING row can become PROCESSED, so it happens at most once."""
        stmt = (
            update(Transaction)
            .where(
                Transaction.transaction_id == transaction_id,
                Transaction.status == TransactionStatus.PROCESSING,
            )
            .values(
                status=TransactionStatus.PROCESSED,
                processed_at=processed_at,
                attempts=Transaction.attempts + 1,
                last_error=None,
            )
        )
        async with self._sessions.begin() as session:
            result = await session.execute(stmt)
        return bool(result.rowcount)  # type: ignore[attr-defined]

    async def record_attempt_error(self, transaction_id: str, error: str) -> None:
        stmt = (
            update(Transaction)
            .where(Transaction.transaction_id == transaction_id)
            .values(attempts=Transaction.attempts + 1, last_error=error[:2000])
        )
        async with self._sessions.begin() as session:
            await session.execute(stmt)

    async def mark_failed(self, transaction_id: str, error: str) -> None:
        stmt = (
            update(Transaction)
            .where(
                Transaction.transaction_id == transaction_id,
                Transaction.status == TransactionStatus.PROCESSING,
            )
            .values(status=TransactionStatus.FAILED, last_error=error[:2000])
        )
        async with self._sessions.begin() as session:
            await session.execute(stmt)
