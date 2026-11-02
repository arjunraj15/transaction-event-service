import asyncio
from collections.abc import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from txn_service.api.dependencies import EventsDep
from txn_service.api.security import AdminDep
from txn_service.events import hello_payload, sse_frame

router = APIRouter(prefix="/v1/events", tags=["events"])

HEARTBEAT_SECONDS = 15.0


@router.get("/stream", response_class=StreamingResponse, dependencies=[AdminDep])
async def event_stream(request: Request, events: EventsDep) -> StreamingResponse:
    """Admin (dashboard): live transaction state changes as Server-Sent Events.

    Read with fetch() rather than EventSource, so the admin key travels in a header, not the URL.
    """
    queue = events.subscribe()

    async def frames() -> AsyncIterator[str]:
        try:
            yield "retry: 3000\n\n"  # browser reconnect delay
            yield sse_frame(hello_payload(events.connected), event="hello")
            while not await request.is_disconnected():
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                except TimeoutError:
                    yield ": heartbeat\n\n"  # keeps proxies from closing an idle stream
                    continue
                if payload is None:  # slow client or shutdown: end the stream, the browser reconnects
                    return
                yield sse_frame(payload)
        finally:
            events.unsubscribe(queue)

    return StreamingResponse(
        frames(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )
