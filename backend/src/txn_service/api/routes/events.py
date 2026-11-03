# Used for the heartbeat timeout.
import asyncio

# Type for the async generator that yields stream frames.
from collections.abc import AsyncIterator

# Router and request objects (the request detects client disconnects).
from fastapi import APIRouter, Request

# Streaming response lets us keep the connection open and push frames.
from fastapi.responses import StreamingResponse

# Dependency giving access to the live event source.
from txn_service.api.dependencies import EventsDep

# Dependency enforcing the admin key.
from txn_service.api.security import AdminDep

# Helpers that format SSE frames and the initial hello payload.
from txn_service.events import hello_payload, sse_frame

# All routes here live under /v1/events.
router = APIRouter(prefix="/v1/events", tags=["events"])

# Send a comment frame this often when idle.
HEARTBEAT_SECONDS = 15.0


# GET /v1/events/stream, admin-only, streamed as SSE.
@router.get("/stream", response_class=StreamingResponse, dependencies=[AdminDep])
# Handler gets the request and the injected event source.
async def event_stream(request: Request, events: EventsDep) -> StreamingResponse:
    """Admin (dashboard): live transaction state changes as Server-Sent Events.

    Read with fetch() rather than EventSource, so the admin key travels in a header, not the URL.
    """
    # Register this client to receive broadcast events.
    queue = events.subscribe()

    # Generator producing the stream's text frames.
    async def frames() -> AsyncIterator[str]:
        # Ensures the subscription is cleaned up however the stream ends.
        try:
            yield "retry: 3000\n\n"  # browser reconnect delay
            # First event: tells the client the upstream connection state.
            yield sse_frame(hello_payload(events.connected), event="hello")
            # Keep streaming until the client disconnects.
            while not await request.is_disconnected():
                # Wait for the next event, but not forever.
                try:
                    # Block until an event arrives or the heartbeat interval passes.
                    payload = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                # No event in time: send a heartbeat instead.
                except TimeoutError:
                    yield ": heartbeat\n\n"  # keeps proxies from closing an idle stream
                    # Go back to waiting.
                    continue
                if payload is None:  # slow client or shutdown: end the stream, the browser reconnects
                    # Stop the generator.
                    return
                # Forward the event to the client as an SSE frame.
                yield sse_frame(payload)
        finally:
            # Always drop this client's queue to avoid leaks.
            events.unsubscribe(queue)

    # Wrap the generator in a streaming HTTP response.
    return StreamingResponse(
        # The frame generator.
        frames(),
        # Content type that browsers use for SSE.
        media_type="text/event-stream",
        # Disable caching and proxy buffering so frames arrive immediately.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )
