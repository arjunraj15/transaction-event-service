import { useCallback, useMemo, useRef, useState } from "react";
import { listTransactions, UnauthorizedError } from "../api";
import type { ServerEvent, Transaction } from "../types";

const MAX_ROWS = 300;

export interface TransactionStore {
  rows: Transaction[]; // newest first
  /** Accepted by the API (202) from this dashboard but not yet consumed by the worker. */
  queued: Map<string, number>;
  reseed: () => Promise<void>;
  applyEvent: (event: ServerEvent) => void;
  markAccepted: (transactionId: string) => void;
}

/** Live view of recent transactions: seeded from REST, then kept current by SSE events. */
export function useTransactions(): TransactionStore {
  const [byId, setById] = useState<Map<string, Transaction>>(new Map());
  const [queued, setQueued] = useState<Map<string, number>>(new Map());
  const seeding = useRef(false);

  const dequeue = (id: string) =>
    setQueued((q) => {
      if (!q.has(id)) return q;
      const next = new Map(q);
      next.delete(id);
      return next;
    });

  const reseed = useCallback(async () => {
    if (seeding.current) return;
    seeding.current = true;
    try {
      const rows = await listTransactions(100);
      setById((prev) => {
        const next = new Map(prev);
        for (const row of rows) next.set(row.transaction_id, row);
        return next;
      });
      setQueued((q) => {
        const next = new Map(q);
        for (const row of rows) next.delete(row.transaction_id);
        return next;
      });
    } catch (err) {
      if (err instanceof UnauthorizedError) throw err; // the caller signs out
      /* other failures: the stream status already shows connectivity problems */
    } finally {
      seeding.current = false;
    }
  }, []);

  const applyEvent = useCallback((event: ServerEvent) => {
    const id = event.transaction_id;
    if (!id) return;
    dequeue(id);
    setById((prev) => {
      const current = prev.get(id);
      const next = new Map(prev);
      switch (event.type) {
        case "ingested":
        case "resumed":
          next.set(id, {
            transaction_id: id,
            source_account: current?.source_account ?? "—",
            destination_account: current?.destination_account ?? "—",
            amount: event.amount !== undefined ? Number(event.amount) : (current?.amount ?? 0),
            currency: event.currency ?? current?.currency ?? "",
            status: "PROCESSING",
            created_at: event.created_at ?? current?.created_at ?? event.at,
            processed_at: null,
          });
          break;
        case "processed":
        case "dead_lettered":
          next.set(id, {
            transaction_id: id,
            source_account: current?.source_account ?? "—",
            destination_account: current?.destination_account ?? "—",
            amount: event.amount !== undefined ? Number(event.amount) : (current?.amount ?? 0),
            currency: event.currency ?? current?.currency ?? "",
            status: event.type === "processed" ? "PROCESSED" : "FAILED",
            created_at: event.created_at ?? current?.created_at ?? event.at,
            processed_at: event.processed_at ?? (event.type === "processed" ? event.at : null),
          });
          break;
        default:
          return prev; // duplicates change nothing: that is the point
      }
      return next;
    });
  }, []);

  const byIdRef = useRef(byId);
  byIdRef.current = byId;
  const markAccepted = useCallback((transactionId: string) => {
    if (byIdRef.current.has(transactionId)) return; // the worker already reported it
    setQueued((q) => (q.has(transactionId) ? q : new Map(q).set(transactionId, Date.now())));
  }, []);

  const rows = useMemo(
    () =>
      [...byId.values()]
        .sort((a, b) => b.created_at.localeCompare(a.created_at))
        .slice(0, MAX_ROWS),
    [byId],
  );

  return { rows, queued, reseed, applyEvent, markAccepted };
}
