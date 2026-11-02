import { useCallback, useState } from "react";
import type { LogEntry, RequestResult, ServerEvent } from "../types";

const MAX_ENTRIES = 1000;
let nextId = 1;

export interface LogStore {
  entries: LogEntry[]; // oldest first
  add: (entry: Omit<LogEntry, "id" | "at"> & { at?: Date }) => void;
  addRequest: (result: RequestResult) => void;
  addServerEvent: (event: ServerEvent) => void;
  clear: () => void;
}

const levelForStatus = (status: number): LogEntry["level"] =>
  status >= 200 && status < 300 ? "ok" : status >= 400 && status < 500 ? "warn" : "error";

function describeEvent(event: ServerEvent): Pick<LogEntry, "level" | "text"> {
  const id = event.transaction_id ?? "—";
  switch (event.type) {
    case "ingested":
      return { level: "info", text: `worker  ingested ${id} → PROCESSING (row written, 30 s timer started)` };
    case "resumed":
      return { level: "warn", text: `worker  resumed ${id} after redelivery (still PROCESSING)` };
    case "duplicate_ignored":
      return { level: "warn", text: `worker  duplicate ${id} ignored (already ${event.status ?? "stored"})` };
    case "processed":
      return { level: "ok", text: `worker  processed ${id} → PROCESSED in ${event.duration_s?.toFixed(2)} s` };
    case "dead_lettered":
      return { level: "error", text: `worker  ${id} → FAILED, sent to DLQ: ${event.detail ?? ""}` };
    case "invalid_message":
      return { level: "error", text: `worker  invalid message dead-lettered: ${event.detail ?? ""}` };
  }
}

export function useLog(): LogStore {
  const [entries, setEntries] = useState<LogEntry[]>([]);

  const add = useCallback((entry: Omit<LogEntry, "id" | "at"> & { at?: Date }) => {
    setEntries((prev) => {
      const next = [...prev, { ...entry, id: nextId++, at: entry.at ?? new Date() }];
      return next.length > MAX_ENTRIES ? next.slice(next.length - MAX_ENTRIES) : next;
    });
  }, []);

  const addRequest = useCallback(
    (r: RequestResult) =>
      add({
        source: "client",
        level: levelForStatus(r.status),
        transactionId: r.transactionId,
        at: r.at,
        text: `client  ${r.method} ${r.path} → ${r.status || "network error"} in ${r.ms.toFixed(0)} ms${
          r.transactionId ? `  [${r.transactionId}]` : ""
        }`,
      }),
    [add],
  );

  const addServerEvent = useCallback(
    (e: ServerEvent) =>
      add({ source: "server", transactionId: e.transaction_id, at: new Date(e.at), ...describeEvent(e) }),
    [add],
  );

  const clear = useCallback(() => setEntries([]), []);
  return { entries, add, addRequest, addServerEvent, clear };
}
