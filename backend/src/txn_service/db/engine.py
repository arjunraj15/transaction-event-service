import ssl
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from txn_service.config import Settings

# libpq-only query params that asyncpg rejects.
_LIBPQ_ONLY_PARAMS = {"sslmode", "channel_binding"}


def normalize_database_url(raw_url: str) -> tuple[str, dict[str, Any]]:
    """Turn a provider URL (postgres://...?sslmode=require) into an asyncpg URL + connect_args."""
    url = raw_url
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            url = "postgresql+asyncpg://" + url[len(prefix) :]
            break

    parts = urlsplit(url)
    params = parse_qsl(parts.query, keep_blank_values=True)
    connect_args: dict[str, Any] = {}
    sslmode = dict(params).get("sslmode")
    if sslmode and sslmode not in ("disable", "allow", "prefer"):
        connect_args["ssl"] = ssl.create_default_context()
    kept = [(k, v) for k, v in params if k not in _LIBPQ_ONLY_PARAMS]
    return urlunsplit(parts._replace(query=urlencode(kept))), connect_args


def create_engine(settings: Settings) -> AsyncEngine:
    url, connect_args = normalize_database_url(settings.database_url)
    return create_async_engine(
        url,
        connect_args=connect_args,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=300,  # serverless Postgres (Neon) drops idle connections
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
