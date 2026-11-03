// React hooks for state, refs and effect lifecycle.
import { useEffect, useRef, useState } from "react";
// apiUrl builds the endpoint URL; adminHeaders supplies the auth header.
import { adminHeaders, apiUrl } from "../api";
// Parser that turns raw stream text into SSE frames.
import { createSSEParser } from "../lib/sse";
import type { ServerEvent, StreamState } from "../types";

// Delay before retrying after the stream drops.
const RECONNECT_MS = 3000;

/**
 * Subscribes to GET /v1/events/stream with fetch() so the admin key goes in a header (EventSource
 * can't send headers, and a key in the URL would end up in proxy logs). Reconnects after drops;
 * `onConnected` fires on every (re)connect so the caller can re-seed state it may have missed.
 */
export function useEventStream(
  // Called for each domain event from the server.
  onEvent: (event: ServerEvent) => void,
  // Called on each (re)connect with whether the API's Postgres listener is up.
  onConnected: (busConnected: boolean) => void,
  // Called when the server rejects the admin key.
  onUnauthorized: () => void,
  // Lets the caller switch the subscription on/off.
  enabled: boolean,
): StreamState {
  // Connection state surfaced to the UI.
  const [state, setState] = useState<StreamState>("connecting");
  // Keep the latest callbacks in a ref so the long-lived stream loop never uses stale closures
  // and the effect below does not need to restart when callers pass new function identities.
  const handlers = useRef({ onEvent, onConnected, onUnauthorized });
  handlers.current = { onEvent, onConnected, onUnauthorized };

  // Open the stream on mount (or when `enabled` changes) and tear it down on cleanup.
  useEffect(() => {
    // Nothing to do while disabled.
    if (!enabled) return;
    // Lets cleanup cancel the in-flight fetch.
    const abort = new AbortController();
    // Pending reconnect timer, cleared on cleanup.
    let retryTimer: ReturnType<typeof setTimeout> | undefined;

    // One connection attempt; on any end it schedules the next attempt.
    const connect = async () => {
      try {
        const res = await fetch(apiUrl("/v1/events/stream"), {
          // Ask for SSE and attach the admin key header (the reason EventSource is not used).
          headers: { Accept: "text/event-stream", ...adminHeaders() },
          // Aborting on unmount closes the connection.
          signal: abort.signal,
          // Never serve a cached stream response.
          cache: "no-store",
        });
        // A bad key is not retryable: notify the app (which signs out) and stop.
        if (res.status === 401) {
          handlers.current.onUnauthorized();
          return;
        }
        // Treat other failures as transient; the catch below triggers a reconnect.
        if (!res.ok || !res.body) throw new Error(`stream HTTP ${res.status}`);
        // Decode bytes to text as they arrive and read them chunk by chunk.
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        // Parser that calls back once per complete SSE frame.
        const push = createSSEParser((frame) => {
          try {
            const data = JSON.parse(frame.data);
            // "hello" is the server's handshake sent on connect.
            if (frame.event === "hello") {
              // The stream is established, so mark it live.
              setState("live");
              // Tell the caller whether the server-side event bus is connected.
              handlers.current.onConnected(Boolean(data.connected));
            } else {
              // Any other frame is a transaction event.
              handlers.current.onEvent(data as ServerEvent);
            }
          } catch {
            /* ignore malformed frames */
          }
        });
        // Read until the server closes the stream.
        for (;;) {
          const { value, done } = await reader.read();
          // Server ended the stream; fall through to the reconnect logic.
          if (done) break;
          // Feed the text chunk to the SSE parser.
          push(value);
        }
      } catch {
        // An abort is our own cleanup, so exit quietly; other errors fall through to reconnect.
        if (abort.signal.aborted) return;
      }
      // Don't reconnect if we've been unmounted/disabled.
      if (abort.signal.aborted) return;
      // Show the UI that we're retrying.
      setState("reconnecting");
      // Try again after a short delay.
      retryTimer = setTimeout(connect, RECONNECT_MS);
    };

    // Kick off the first connection (promise intentionally not awaited).
    void connect();
    // Cleanup on unmount/disable.
    return () => {
      // Cancel the in-flight request and stop reading.
      abort.abort();
      // Cancel any scheduled reconnect.
      clearTimeout(retryTimer);
    };
  }, [enabled]);

  // When disabled, report "connecting" rather than a stale state.
  return enabled ? state : "connecting";
}
