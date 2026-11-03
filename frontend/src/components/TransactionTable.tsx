// State hooks for filters and paging.
import { useEffect, useState } from "react";
// Paged REST fetcher.
import { listTransactions } from "../api";
// Time formatter, aliased to `time` for use in the table cells.
import { clockTime as time } from "../lib/time";
// Row type returned by the transactions API.
import type { Transaction } from "../types";

// Inputs from the parent.
interface Props {
  // Live view of the newest transactions (newest first); used for page 1.
  rows: Transaction[];
  // Whole-table counts per status from the server; drive chip counts and page count.
  totals: Record<string, number>;
  // Current time in ms, ticked by the parent so progress bars animate.
  now: number;
  // Simulated processing delay, used to scale progress bars.
  delaySeconds: number;
  // Ids sent from this browser session, for highlighting/filtering.
  sessionIds: Set<string>;
  // Opens the inspect drawer for a transaction.
  onInspect: (transactionId: string) => void;
}

// Status filter options.
type Filter = "all" | "progress" | "done";

// Rows per page, fetched from the server one page at a time.
const PAGE_SIZE = 100;
// Statuses each filter maps to (empty = no filter).
const FILTER_STATUSES: Record<Filter, string[]> = {
  all: [],
  progress: ["PROCESSING"],
  done: ["PROCESSED", "FAILED"],
};

// Filterable, server-paginated table of transactions with per-row progress for those still processing.
export function TransactionTable({ rows, totals, now, delaySeconds, sessionIds, onInspect }: Props) {
  // Which status subset to show.
  const [filter, setFilter] = useState<Filter>("all");
  // Restrict this page to transactions created from this session.
  const [onlyMine, setOnlyMine] = useState(false);
  // Zero-based page index.
  const [page, setPage] = useState(0);
  // Rows of the current page when it is not page 1 (page 1 comes from the live store).
  const [fetched, setFetched] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(false);

  // Whole-table counts for each filter.
  const sum = (statuses: string[]) => statuses.reduce((n, s) => n + (totals[s] ?? 0), 0);
  const counts: Record<Filter, number> = {
    all: Object.values(totals).reduce((n, v) => n + v, 0),
    progress: sum(FILTER_STATUSES.progress),
    done: sum(FILTER_STATUSES.done),
  };
  const lastPage = Math.max(0, Math.ceil(counts[filter] / PAGE_SIZE) - 1);
  // Stay in range if the total shrinks.
  const current = Math.min(page, lastPage);

  // Fetch only the requested page from the server (page 1 is served live from the store).
  useEffect(() => {
    if (current === 0) return;
    let cancelled = false;
    setLoading(true);
    listTransactions(PAGE_SIZE, current * PAGE_SIZE, FILTER_STATUSES[filter])
      .then((r) => !cancelled && setFetched(r))
      .catch(() => undefined)
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, [current, filter, counts[filter]]);

  const statuses = FILTER_STATUSES[filter];
  const base =
    current === 0
      ? rows.filter((r) => statuses.length === 0 || statuses.includes(r.status)).slice(0, PAGE_SIZE)
      : fetched;
  // Optionally limit to this session.
  const shown = onlyMine ? base.filter((r) => sessionIds.has(r.transaction_id)) : base;

  return (
    <section className="card table-card">
      {/* Header with title and filter controls */}
      <div className="card-head">
        <h2>Transactions</h2>
        <div className="filters">
          {/* Filter chips with live counts; `as const` types the ids as Filter */}
          {(
            [
              ["all", `All (${counts.all})`],
              ["progress", `In progress (${counts.progress})`],
              ["done", `Completed (${counts.done})`],
            ] as const
          ).map(([id, label]) => (
            // Highlight the selected chip; switching filter returns to page 1
            <button
              key={id}
              className={filter === id ? "chip active" : "chip"}
              onClick={() => {
                setFilter(id);
                setPage(0);
              }}
            >
              {label}
            </button>
          ))}
          {/* Toggle to show only this session's transactions */}
          <label className="check">
            <input type="checkbox" checked={onlyMine} onChange={(e) => setOnlyMine(e.target.checked)} />
            only this session
          </label>
        </div>
      </div>
      {/* Scroll container for the table */}
      <div className="table-wrap">
        <table>
          {/* Column headings; numeric columns right-aligned via "num" */}
          <thead>
            <tr>
              <th>transaction_id</th>
              <th className="num">amount</th>
              <th>status</th>
              <th>created_at</th>
              <th>processed_at</th>
              <th className="num">duration</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((r) => {
              // Seconds since creation, for the progress bar.
              const elapsed = (now - Date.parse(r.created_at)) / 1000;
              // Total processing time in seconds, or null if not finished.
              const duration = r.processed_at
                ? (Date.parse(r.processed_at) - Date.parse(r.created_at)) / 1000
                : null;
              return (
                <tr
                  // Transaction ids are unique, so they make stable keys.
                  key={r.transaction_id}
                  // Highlight rows created in this session.
                  className={sessionIds.has(r.transaction_id) ? "mine" : ""}
                  // Clicking a row opens the inspect drawer.
                  onClick={() => onInspect(r.transaction_id)}
                  title="Click to see GET /v1/transactions/{id}"
                >
                  {/* Transaction id */}
                  <td className="mono">{r.transaction_id}</td>
                  {/* Amount with locale formatting (up to 4 decimals) and currency */}
                  <td className="num mono">
                    {r.amount.toLocaleString(undefined, { maximumFractionDigits: 4 })} {r.currency}
                  </td>
                  {/* Status badge, plus a mini progress bar while processing */}
                  <td>
                    <span className={`badge badge-${r.status.toLowerCase()}`}>{r.status}</span>
                    {r.status === "PROCESSING" && (
                      <span className="mini-bar">
                        {/* Fill width = elapsed / delay, capped at 100% */}
                        <span style={{ width: `${Math.min(100, (elapsed / delaySeconds) * 100)}%` }} />
                      </span>
                    )}
                  </td>
                  {/* Creation time */}
                  <td className="mono">{time(r.created_at)}</td>
                  {/* Completion time (formatter handles null) */}
                  <td className="mono">{time(r.processed_at)}</td>
                  {/* Duration, or a dash if still processing */}
                  <td className="num mono">{duration !== null ? `${duration.toFixed(2)} s` : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {/* Empty-state message when nothing matches */}
        {shown.length === 0 && <p className="empty">{loading ? "Loading…" : "No transactions to show yet."}</p>}
      </div>
      {/* Pager */}
      <div className="pager">
        <button className="chip" disabled={current === 0} onClick={() => setPage(current - 1)}>
          ‹ Prev
        </button>
        <span>
          Page {current + 1} of {lastPage + 1}
        </span>
        <button className="chip" disabled={current >= lastPage} onClick={() => setPage(current + 1)}>
          Next ›
        </button>
      </div>
    </section>
  );
}
