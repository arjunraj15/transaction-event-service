"""create transactions table

Revision ID: 0001
Revises:
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("transaction_id", sa.String(128), primary_key=True),
        sa.Column("source_account", sa.String(128), nullable=False),
        sa.Column("destination_account", sa.String(128), nullable=False),
        sa.Column("amount", sa.Numeric(18, 4), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('PROCESSING', 'PROCESSED', 'FAILED')", name="ck_transactions_status"),
    )
    op.create_index("ix_transactions_status_created_at", "transactions", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_transactions_status_created_at", table_name="transactions")
    op.drop_table("transactions")
