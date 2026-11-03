# TLS context for secure database connections.
import ssl

# Type for connect_args.
from typing import Any

# URL parsing helpers.
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Async engine/session classes and factories.
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

# Settings model.
from txn_service.config import Settings

# libpq-only query params that asyncpg rejects.
_LIBPQ_ONLY_PARAMS = {"sslmode", "channel_binding"}


# Convert a provider URL into one asyncpg understands.
def normalize_database_url(raw_url: str) -> tuple[str, dict[str, Any]]:
    """Turn a provider URL (postgres://...?sslmode=require) into an asyncpg URL + connect_args."""
    # Start from the raw URL.
    url = raw_url
    # Provider URLs may use either scheme prefix.
    for prefix in ("postgres://", "postgresql://"):
        # If the URL starts with this prefix...
        if url.startswith(prefix):
            # ...swap it for the asyncpg driver scheme.
            url = "postgresql+asyncpg://" + url[len(prefix) :]
            # Stop after the first matching prefix.
            break

    # Split the URL into components.
    parts = urlsplit(url)
    # Parse the query string into key/value pairs.
    params = parse_qsl(parts.query, keep_blank_values=True)
    # Driver-specific options passed separately from the URL.
    connect_args: dict[str, Any] = {}
    # Look up the requested SSL mode, if any.
    sslmode = dict(params).get("sslmode")
    # Any mode other than these means TLS is wanted.
    if sslmode and sslmode not in ("disable", "allow", "prefer"):
        # Enable TLS with a default verifying context.
        connect_args["ssl"] = ssl.create_default_context()
    # Drop the parameters asyncpg does not accept.
    kept = [(k, v) for k, v in params if k not in _LIBPQ_ONLY_PARAMS]
    # Rebuild the URL and return it with the connect args.
    return urlunsplit(parts._replace(query=urlencode(kept))), connect_args


# Create the async engine with connection pooling.
def create_engine(settings: Settings) -> AsyncEngine:
    # Normalize the configured URL.
    url, connect_args = normalize_database_url(settings.database_url)
    # Build the engine.
    return create_async_engine(
        # The cleaned URL.
        url,
        # Pass TLS options.
        connect_args=connect_args,
        # Number of persistent pooled connections.
        pool_size=settings.db_pool_size,
        # Extra connections allowed under burst load.
        max_overflow=settings.db_max_overflow,
        # Test each connection before use to avoid stale ones.
        pool_pre_ping=True,
        pool_recycle=300,  # serverless Postgres (Neon) drops idle connections
    )


# Create a session factory bound to the engine.
def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # Don't expire objects after commit, so they stay readable once detached.
    return async_sessionmaker(engine, expire_on_commit=False)
