"""Live-event bus against real Postgres: worker-side pg_notify reaches API-side LISTEN subscribers."""

import asyncio
import json
from datetime import UTC, datetime

import pytest

from tests.integration.conftest import Infra
from txn_service.config import Settings
from txn_service.db.engine import create_engine
from txn_service.events import EventBroadcaster, PgNotifyEventSink, TxnEvent, TxnEventType

pytestmark = pytest.mark.integration


async def test_notify_reaches_broadcaster_subscribers(infra: Infra) -> None:
    channel = "txn_events_it"
    broadcaster = EventBroadcaster(infra.database_url, channel)
    await broadcaster.start()
    engine = create_engine(Settings(database_url=infra.database_url))
    try:
        async with asyncio.timeout(15):
            while not broadcaster.connected:
                await asyncio.sleep(0.1)
        queue = broadcaster.subscribe()

        sink = PgNotifyEventSink(engine, channel)
        await sink.emit(
            TxnEvent(
                type=TxnEventType.PROCESSED, at=datetime.now(UTC), transaction_id="txn_it", duration_s=30.0
            )
        )

        payload = await asyncio.wait_for(queue.get(), timeout=10)
        await sink.close()
        assert payload is not None
        assert json.loads(payload) | {"at": None} == {
            "type": "processed",
            "transaction_id": "txn_it",
            "duration_s": 30.0,
            "at": None,
        }
    finally:
        await broadcaster.stop()
        await engine.dispose()
