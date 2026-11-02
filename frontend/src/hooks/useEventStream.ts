import { useEffect, useRef, useState } from "react";
import { adminHeaders, apiUrl } from "../api";
import { createSSEParser } from "../lib/sse";
import type { ServerEvent, StreamState } from "../types";

const RECONNECT_MS = 3000;

/**
 * Subscribes to GET /v1/events/stream with fetch() so the admin key goes in a header (EventSource
 * can't send headers, and a key in the URL would end up in proxy logs). Reconnects after drops;
 * `onConnected` fires on every (re)connect so the caller can re-seed state it may have missed.
 */
export function useEventStream(
  onEvent: (event: ServerEvent) => void,
  onConnected: (busConnected: boolean) => void,
  onUnauthorized: () => void,
  enabled: boolean,
): StreamState {
  const [state, setState] = useState<StreamState>("connecting");
  const handlers = useRef({ onEvent, onConnected, onUnauthorized });
  handlers.current = { onEvent, onConnected, onUnauthorized };

  useEffect(() => {
    if (!enabled) return;
    const abort = new AbortController();
    let retryTimer: ReturnType<typeof setTimeout> | undefined;

    const connect = async () => {
      try {
        const res = await fetch(apiUrl("/v1/events/stream"), {
          headers: { Accept: "text/event-stream", ...adminHeaders() },
          signal: abort.signal,
          cache: "no-store",
        });
        if (res.status === 401) {
          handlers.current.onUnauthorized();
          return;
        }
        if (!res.ok || !res.body) throw new Error(`stream HTTP ${res.status}`);
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        const push = createSSEParser((frame) => {
          try {
            const data = JSON.parse(frame.data);
            if (frame.event === "hello") {
              setState("live");
              handlers.current.onConnected(Boolean(data.connected));
            } else {
              handlers.current.onEvent(data as ServerEvent);
            }
          } catch {
            /* ignore malformed frames */
          }
        });
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          push(value);
        }
      } catch {
        if (abort.signal.aborted) return;
      }
      if (abort.signal.aborted) return;
      setState("reconnecting");
      retryTimer = setTimeout(connect, RECONNECT_MS);
    };

    void connect();
    return () => {
      abort.abort();
      clearTimeout(retryTimer);
    };
  }, [enabled]);

  return enabled ? state : "connecting";
}
