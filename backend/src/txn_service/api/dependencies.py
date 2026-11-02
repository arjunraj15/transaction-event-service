import asyncio
from typing import Annotated, Protocol

from fastapi import Depends, Request

from txn_service.config import Settings
from txn_service.db.models import Transaction
from txn_service.messaging.schemas import TransactionEvent


class Publisher(Protocol):
    async def publish(self, event: TransactionEvent) -> None: ...
    async def is_ready(self) -> bool: ...


class TransactionReader(Protocol):
    async def get(self, transaction_id: str) -> Transaction | None: ...
    async def list_recent(self, limit: int, status: str | None = None) -> list[Transaction]: ...
    async def ping(self) -> bool: ...


class EventSource(Protocol):
    connected: bool

    def subscribe(self) -> asyncio.Queue[str | None]: ...
    def unsubscribe(self, queue: asyncio.Queue[str | None]) -> None: ...


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_publisher(request: Request) -> Publisher:
    publisher: Publisher = request.app.state.publisher
    return publisher


def get_reader(request: Request) -> TransactionReader:
    reader: TransactionReader = request.app.state.reader
    return reader


def get_events(request: Request) -> EventSource:
    events: EventSource = request.app.state.events
    return events


SettingsDep = Annotated[Settings, Depends(get_settings)]
EventsDep = Annotated[EventSource, Depends(get_events)]
PublisherDep = Annotated[Publisher, Depends(get_publisher)]
ReaderDep = Annotated[TransactionReader, Depends(get_reader)]
