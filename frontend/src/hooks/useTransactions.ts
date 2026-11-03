// React hooks for state, memoization and refs.
import { useCallback, useMemo, useRef, useState } from "react";
// REST fetcher plus the error type signalling a rejected admin key.
import { getStats, listTransactions, UnauthorizedError } from "../api";
import type { ServerEvent, Transaction } from "../types";

// Cap on rows shown so the table and memo work stay bounded.
const MAX_ROWS = 300;

// What useTransactions returns.
export interface TransactionStore {
  rows: Transaction[]; // newest first
  /** Accepted by the API (202) from this dashboard but not yet consumed by the worker. */
  queued: Map<string, number>;
  /** Whole-table counts per status; may lag live events until the next reseed. */
  totals: Record<string, number>;
  // Re-fetch recent transactions from REST and merge them in.
  reseed: () => Promise<void>;
  // Fold a live stream event into the row state.
  applyEvent: (event: ServerEvent) => void;
  // Record that the API accepted a webhook, so it shows as queued until the worker reports it.
  markAccepted: (transactionId: string) => void;
}

/** Live view of recent transactions: seeded from REST, then kept current by SSE events. */
export function useTransactions(): TransactionStore {
  // All known transactions keyed by id, so updates are O(1) and idempotent.
  const [byId, setById] = useState<Map<string, Transaction>>(new Map());
  // id -> time accepted, for transactions the API took but the worker has not yet picked up.
  const [queued, setQueued] = useState<Map<string, number>>(new Map());
  // Whole-table totals per status from the server (the row list is capped, the totals are not).
  const [totals, setTotals] = useState<Record<string, number>>({});
  // Guards against overlapping reseed calls (a ref, so it doesn't trigger renders).
  const seeding = useRef(false);

  // Remove an id from the queued set (the worker has now seen it).
  const dequeue = (id: string) =>
    setQueued((q) => {
      // Return the same Map when nothing changes so React skips a re-render.
      if (!q.has(id)) return q;
      // Copy before mutating: React state must be immutable.
      const next = new Map(q);
      next.delete(id);
      return next;
    });

  // Stable callback; safe to put in effect dependency lists.
  const reseed = useCallback(async () => {
    // Skip if a reseed is already in flight.
    if (seeding.current) return;
    seeding.current = true;
    try {
      const [rows, stats] = await Promise.all([listTransactions(100), getStats().catch(() => null)]);
      if (stats) setTotals(stats);
      // Merge rather than replace, so rows beyond the fetched page (or newer from SSE) are kept.
      setById((prev) => {
        const next = new Map(prev);
        // REST rows overwrite local copies, which fixes anything a missed event left stale.
        for (const row of rows) next.set(row.transaction_id, row);
        return next;
      });
      // Anything the server now lists is no longer "queued".
      setQueued((q) => {
        const next = new Map(q);
        for (const row of rows) next.delete(row.transaction_id);
        return next;
      });
    } catch (err) {
      if (err instanceof UnauthorizedError) throw err; // the caller signs out
      /* other failures: the stream status already shows connectivity problems */
    } finally {
      // Always release the guard, even on failure.
      seeding.current = false;
    }
  }, []);

  // Stable callback applying one stream event to local state.
  const applyEvent = useCallback((event: ServerEvent) => {
    const id = event.transaction_id;
    // Events without a transaction (e.g. invalid messages) don't affect any row.
    if (!id) return;
    // The worker has seen this transaction, so it is no longer waiting in the queue.
    dequeue(id);
    setById((prev) => {
      // Existing row, if any, used to preserve fields the event doesn't carry.
      const current = prev.get(id);
      const next = new Map(prev);
      switch (event.type) {
        // A row was created (or re-picked up): it is in flight.
        case "ingested":
        case "resumed":
          next.set(id, {
            transaction_id: id,
            // Events don't include accounts, so keep any known value or show a placeholder until reseed.
            source_account: current?.source_account ?? "—",
            destination_account: current?.destination_account ?? "—",
            // Event amount is a string; convert, falling back to the known value or 0.
            amount: event.amount !== undefined ? Number(event.amount) : (current?.amount ?? 0),
            currency: event.currency ?? current?.currency ?? "",
            status: "PROCESSING",
            // Prefer the server's creation time, then the known one, then the event time.
            created_at: event.created_at ?? current?.created_at ?? event.at,
            // Not finished yet.
            processed_at: null,
          });
          break;
        // Terminal outcomes: finished or dead-lettered.
        case "processed":
        case "dead_lettered":
          next.set(id, {
            transaction_id: id,
            source_account: current?.source_account ?? "—",
            destination_account: current?.destination_account ?? "—",
            amount: event.amount !== undefined ? Number(event.amount) : (current?.amount ?? 0),
            currency: event.currency ?? current?.currency ?? "",
            // Success maps to PROCESSED, a dead-letter to FAILED.
            status: event.type === "processed" ? "PROCESSED" : "FAILED",
            created_at: event.created_at ?? current?.created_at ?? event.at,
            // Failed rows have no completion time.
            processed_at: event.processed_at ?? (event.type === "processed" ? event.at : null),
          });
          break;
        default:
          return prev; // duplicates change nothing: that is the point
      }
      return next;
    });
  }, []);

  // Mirror of byId in a ref so markAccepted can read the latest value without changing identity.
  const byIdRef = useRef(byId);
  byIdRef.current = byId;
  // Stable callback invoked when the API returns 202 for a webhook sent from this dashboard.
  const markAccepted = useCallback((transactionId: string) => {
    if (byIdRef.current.has(transactionId)) return; // the worker already reported it
    // Add to the queued map with the acceptance time (no-op if already queued).
    setQueued((q) => (q.has(transactionId) ? q : new Map(q).set(transactionId, Date.now())));
  }, []);

  // Derived, sorted, capped list; recomputed only when the underlying map changes.
  const rows = useMemo(
    () =>
      [...byId.values()]
        // Newest first (ISO timestamps sort correctly as strings).
        .sort((a, b) => b.created_at.localeCompare(a.created_at))
        // Keep only the most recent MAX_ROWS.
        .slice(0, MAX_ROWS),
    [byId],
  );

  // Expose the store.
  return { rows, queued, totals, reseed, applyEvent, markAccepted };
}
