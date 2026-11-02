import asyncio
import json
from datetime import UTC, datetime

from txn_service.events import EventBroadcaster, TxnEvent, TxnEventType, sse_frame


def make_broadcaster(queue_size: int = 10) -> EventBroadcaster:
    return EventBroadcaster("postgres://u:p@h/db?sslmode=require", "txn_events", queue_size=queue_size)


def test_broadcaster_uses_plain_asyncpg_dsn_with_ssl() -> None:
    b = make_broadcaster()
    assert b._dsn == "postgresql://u:p@h/db"
    assert b._ssl is not None


async def test_publish_fans_out_to_every_subscriber() -> None:
    b = make_broadcaster()
    q1, q2 = b.subscribe(), b.subscribe()
    b.publish("a")
    assert q1.get_nowait() == "a" and q2.get_nowait() == "a"
    b.unsubscribe(q1)
    b.publish("b")
    assert q1.empty() and q2.get_nowait() == "b"


async def test_slow_subscriber_is_dropped_with_end_of_stream_sentinel() -> None:
    b = make_broadcaster(queue_size=2)
    slow, fast = b.subscribe(), b.subscribe()
    b.publish("1")
    b.publish("2")
    fast.get_nowait()
    fast.get_nowait()
    b.publish("3")  # slow is full -> dropped
    assert slow.get_nowait() is None
    b.publish("4")
    assert slow.empty()
    assert [fast.get_nowait(), fast.get_nowait()] == ["3", "4"]


async def test_stop_ends_open_streams() -> None:
    b = make_broadcaster()
    q = b.subscribe()
    await b.stop()
    assert await asyncio.wait_for(q.get(), 1) is None


def test_payload_is_compact_and_truncated() -> None:
    e = TxnEvent(type=TxnEventType.DEAD_LETTERED, at=datetime(2024, 1, 1, tzinfo=UTC), detail="x" * 5000)
    data = json.loads(e.to_payload())
    assert data["type"] == "dead_lettered" and len(data["detail"]) == 500
    assert "amount" not in data  # None fields are omitted


def test_sse_frame_format() -> None:
    assert sse_frame('{"a":1}') == 'data: {"a":1}\n\n'
    assert sse_frame("x", event="hello") == "event: hello\ndata: x\n\n"
