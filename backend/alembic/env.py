import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from txn_service.config import get_settings
from txn_service.db.engine import normalize_database_url
from txn_service.db.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _url() -> tuple[str, dict[str, object]]:
    # DATABASE_URL from the environment is the single source of truth (no URL in alembic.ini).
    return normalize_database_url(get_settings().database_url)


def run_migrations_offline() -> None:
    url, _ = _url()
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def _run(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    url, connect_args = _url()
    engine = create_async_engine(url, connect_args=connect_args)
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
