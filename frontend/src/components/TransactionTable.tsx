import { useState } from "react";
import { clockTime as time } from "../lib/time";
import type { Transaction } from "../types";

interface Props {
  rows: Transaction[];
  now: number;
  delaySeconds: number;
  sessionIds: Set<string>;
  onInspect: (transactionId: string) => void;
}

type Filter = "all" | "progress" | "done";


export function TransactionTable({ rows, now, delaySeconds, sessionIds, onInspect }: Props) {
  const [filter, setFilter] = useState<Filter>("all");
  const [onlyMine, setOnlyMine] = useState(false);

  const inProgress = rows.filter((r) => r.status === "PROCESSING");
  const done = rows.filter((r) => r.status !== "PROCESSING");
  const base = filter === "progress" ? inProgress : filter === "done" ? done : rows;
  const shown = (onlyMine ? base.filter((r) => sessionIds.has(r.transaction_id)) : base).slice(0, 150);

  return (
    <section className="card table-card">
      <div className="card-head">
        <h2>Transactions</h2>
        <div className="filters">
          {(
            [
              ["all", `All (${rows.length})`],
              ["progress", `In progress (${inProgress.length})`],
              ["done", `Completed (${done.length})`],
            ] as const
          ).map(([id, label]) => (
            <button key={id} className={filter === id ? "chip active" : "chip"} onClick={() => setFilter(id)}>
              {label}
            </button>
          ))}
          <label className="check">
            <input type="checkbox" checked={onlyMine} onChange={(e) => setOnlyMine(e.target.checked)} />
            only this session
          </label>
        </div>
      </div>
      <div className="table-wrap">
        <table>
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
              const elapsed = (now - Date.parse(r.created_at)) / 1000;
              const duration = r.processed_at
                ? (Date.parse(r.processed_at) - Date.parse(r.created_at)) / 1000
                : null;
              return (
                <tr
                  key={r.transaction_id}
                  className={sessionIds.has(r.transaction_id) ? "mine" : ""}
                  onClick={() => onInspect(r.transaction_id)}
                  title="Click to see GET /v1/transactions/{id}"
                >
                  <td className="mono">{r.transaction_id}</td>
                  <td className="num mono">
                    {r.amount.toLocaleString(undefined, { maximumFractionDigits: 4 })} {r.currency}
                  </td>
                  <td>
                    <span className={`badge badge-${r.status.toLowerCase()}`}>{r.status}</span>
                    {r.status === "PROCESSING" && (
                      <span className="mini-bar">
                        <span style={{ width: `${Math.min(100, (elapsed / delaySeconds) * 100)}%` }} />
                      </span>
                    )}
                  </td>
                  <td className="mono">{time(r.created_at)}</td>
                  <td className="mono">{time(r.processed_at)}</td>
                  <td className="num mono">{duration !== null ? `${duration.toFixed(2)} s` : "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {shown.length === 0 && <p className="empty">No transactions to show yet.</p>}
      </div>
    </section>
  );
}
