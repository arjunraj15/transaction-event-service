// React hooks: state plus memoized callbacks.
import { useCallback, useState } from "react";
// Log/request/event types.
import type { LogEntry, RequestResult, ServerEvent } from "../types";

// Cap on retained log lines so a long session cannot grow memory unboundedly.
const MAX_ENTRIES = 1000;
// Module-level counter for unique entry ids (shared across hook instances).
let nextId = 1;

// What useLog returns: the entries plus the ways to append to or clear them.
export interface LogStore {
  entries: LogEntry[]; // oldest first
  // Append a raw entry; id is assigned and `at` defaults to now.
  add: (entry: Omit<LogEntry, "id" | "at"> & { at?: Date }) => void;
  // Log the outcome of an HTTP call made by the dashboard.
  addRequest: (result: RequestResult) => void;
  // Log an event received from the live stream.
  addServerEvent: (event: ServerEvent) => void;
  // Empty the log.
  clear: () => void;
}

// Map an HTTP status to a severity: 2xx ok, 4xx warn (client mistake), everything else error.
const levelForStatus = (status: number): LogEntry["level"] =>
  status >= 200 && status < 300 ? "ok" : status >= 400 && status < 500 ? "warn" : "error";

// Turn a server event into a severity and a readable line.
function describeEvent(event: ServerEvent): Pick<LogEntry, "level" | "text"> {
  // Fall back to a dash for events without a transaction id.
  const id = event.transaction_id ?? "—";
  switch (event.type) {
    // Worker stored a new row and started the processing timer.
    case "ingested":
      return { level: "info", text: `worker  ingested ${id} → PROCESSING (row written, 30 s timer started)` };
    // Message was redelivered after a crash/restart; row is still in flight.
    case "resumed":
      return { level: "warn", text: `worker  resumed ${id} after redelivery (still PROCESSING)` };
    // Idempotency in action: repeated message did nothing.
    case "duplicate_ignored":
      return { level: "warn", text: `worker  duplicate ${id} ignored (already ${event.status ?? "stored"})` };
    // Success; duration_s is optional, so it may print as undefined if absent.
    case "processed":
      return { level: "ok", text: `worker  processed ${id} → PROCESSED in ${event.duration_s?.toFixed(2)} s` };
    // Retries exhausted: row marked FAILED and message parked in the DLQ.
    case "dead_lettered":
      return { level: "error", text: `worker  ${id} → FAILED, sent to DLQ: ${event.detail ?? ""}` };
    // Unparseable message: no transaction to attribute it to.
    case "invalid_message":
      return { level: "error", text: `worker  invalid message dead-lettered: ${event.detail ?? ""}` };
  }
}

// Holds the live log in component state and exposes stable append/clear callbacks.
export function useLog(): LogStore {
  const [entries, setEntries] = useState<LogEntry[]>([]);

  // Stable callback (empty deps) so consumers' effects/memos do not re-run on every render.
  const add = useCallback((entry: Omit<LogEntry, "id" | "at"> & { at?: Date }) => {
    // Functional update so concurrent appends never overwrite each other.
    setEntries((prev) => {
      // Assign a unique id and default the timestamp to now.
      const next = [...prev, { ...entry, id: nextId++, at: entry.at ?? new Date() }];
      // Drop the oldest lines once over the cap.
      return next.length > MAX_ENTRIES ? next.slice(next.length - MAX_ENTRIES) : next;
    });
  }, []);

  // Format an HTTP result as a client-side log line.
  const addRequest = useCallback(
    (r: RequestResult) =>
      add({
        source: "client",
        // Colour by HTTP status class.
        level: levelForStatus(r.status),
        transactionId: r.transactionId,
        // Use the request's own completion time rather than when it was logged.
        at: r.at,
        // Status 0 means the request never got a response.
        text: `client  ${r.method} ${r.path} → ${r.status || "network error"} in ${r.ms.toFixed(0)} ms${
          r.transactionId ? `  [${r.transactionId}]` : ""
        }`,
      }),
    [add],
  );

  // Format a stream event as a server-side log line, stamped with the server's event time.
  const addServerEvent = useCallback(
    (e: ServerEvent) =>
      add({ source: "server", transactionId: e.transaction_id, at: new Date(e.at), ...describeEvent(e) }),
    [add],
  );

  // Reset to an empty log.
  const clear = useCallback(() => setEntries([]), []);
  // Expose the store.
  return { entries, add, addRequest, addServerEvent, clear };
}
