# Needed for the asyncio.Queue type in the EventSource protocol.
import asyncio

# Annotated attaches dependency metadata; Protocol defines structural interfaces.
from typing import Annotated, Protocol

# Depends wires dependencies into routes; Request gives access to app state.
from fastapi import Depends, Request

# Settings type used by the settings dependency.
from txn_service.config import Settings

# ORM model returned by reader methods.
from txn_service.db.models import Transaction

# Event type handed to the publisher.
from txn_service.messaging.schemas import TransactionEvent


# Interface for anything that can publish a transaction event (Kafka or a test fake).
class Publisher(Protocol):
    # Send one event to the broker.
    async def publish(self, event: TransactionEvent) -> None: ...
    # Report whether the broker connection is usable.
    async def is_ready(self) -> bool: ...


# Interface for reading transactions (Postgres repository or a test fake).
class TransactionReader(Protocol):
    # Fetch one transaction by id, or None.
    async def get(self, transaction_id: str) -> Transaction | None: ...
    # Fetch the newest transactions, optionally filtered by status.
    async def list_recent(
        self, limit: int, status: list[str] | None = None, offset: int = 0
    ) -> list[Transaction]: ...
    # Total rows per status across the whole table.
    async def count_by_status(self) -> dict[str, int]: ...
    # Report whether the database is reachable.
    async def ping(self) -> bool: ...


# Interface for the live event stream feeding SSE clients.
class EventSource(Protocol):
    # Whether the upstream listener is currently connected.
    connected: bool

    # Register a client and get its queue (None is the end-of-stream sentinel).
    def subscribe(self) -> asyncio.Queue[str | None]: ...
    # Remove a client's queue when it disconnects.
    def unsubscribe(self, queue: asyncio.Queue[str | None]) -> None: ...


# Dependency: settings stored on the app at startup.
def get_settings(request: Request) -> Settings:
    # Read settings from app state; the annotation gives the type for mypy.
    settings: Settings = request.app.state.settings
    # Return the settings.
    return settings


# Dependency: the publisher stored on the app at startup.
def get_publisher(request: Request) -> Publisher:
    # Read the publisher from app state.
    publisher: Publisher = request.app.state.publisher
    # Return the publisher.
    return publisher


# Dependency: the transaction reader stored on the app at startup.
def get_reader(request: Request) -> TransactionReader:
    # Read the reader from app state.
    reader: TransactionReader = request.app.state.reader
    # Return the reader.
    return reader


# Dependency: the live event source stored on the app at startup.
def get_events(request: Request) -> EventSource:
    # Read the event source from app state.
    events: EventSource = request.app.state.events
    # Return the event source.
    return events


# Shorthand so routes can just annotate a parameter with SettingsDep.
SettingsDep = Annotated[Settings, Depends(get_settings)]
# Shorthand dependency for the live event source.
EventsDep = Annotated[EventSource, Depends(get_events)]
# Shorthand dependency for the publisher.
PublisherDep = Annotated[Publisher, Depends(get_publisher)]
# Shorthand dependency for the transaction reader.
ReaderDep = Annotated[TransactionReader, Depends(get_reader)]
