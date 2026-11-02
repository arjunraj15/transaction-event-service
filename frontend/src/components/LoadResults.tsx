import { histogram, summarize } from "../lib/stats";
import type { BurstRun } from "./ActionPanel";

const W = 360;
const H = 90;

/** Latency summary and histogram for the last load burst (browser-measured, includes network). */
export function LoadResults({ run }: { run: BurstRun }) {
  const latencies = run.results.map((r) => r.ms);
  const summary = summarize(latencies);
  const buckets = histogram(latencies, 24);
  const peak = Math.max(1, ...buckets.map((b) => b.count));
  const byStatus = run.results.reduce<Record<string, number>>((acc, r) => {
    const key = String(r.status || "ERR");
    acc[key] = (acc[key] ?? 0) + 1;
    return acc;
  }, {});
  const elapsed = ((run.finishedAt ?? Date.now()) - run.startedAt) / 1000;
  const ok = byStatus["202"] ?? 0;

  return (
    <section className="card load-results">
      <div className="card-head">
        <h2>Load burst</h2>
        <span className="hint">
          {run.label} · {run.results.length}/{run.total} · {elapsed.toFixed(1)} s
          {run.finishedAt === null && " · running…"}
        </span>
      </div>
      <div className="stat-row">
        <div className={`stat ${ok === run.results.length && ok > 0 ? "good" : "bad"}`}>
          <span className="stat-value">
            {ok}/{run.results.length}
          </span>
          <span className="stat-label">202 Accepted</span>
        </div>
        {(["p50", "p95", "max"] as const).map((k) => (
          <div key={k} className={`stat ${k !== "max" && summary && summary[k] < 500 ? "good" : ""}`}>
            <span className="stat-value">{summary ? `${summary[k].toFixed(0)} ms` : "—"}</span>
            <span className="stat-label">{k}</span>
          </div>
        ))}
      </div>
      {buckets.length > 0 && (
        <svg className="histogram" viewBox={`0 0 ${W} ${H + 18}`} role="img" aria-label="latency histogram">
          {buckets.map((b, i) => {
            const bw = W / buckets.length;
            const h = (b.count / peak) * H;
            return (
              <rect key={i} x={i * bw + 1} y={H - h} width={Math.max(1, bw - 2)} height={h} rx={2}>
                <title>
                  {b.from.toFixed(0)}–{b.to.toFixed(0)} ms: {b.count}
                </title>
              </rect>
            );
          })}
          <text x={0} y={H + 14}>
            {buckets[0].from.toFixed(0)} ms
          </text>
          <text x={W} y={H + 14} textAnchor="end">
            {buckets[buckets.length - 1].to.toFixed(0)} ms
          </text>
        </svg>
      )}
      {Object.keys(byStatus).some((s) => s !== "202") && (
        <p className="note">
          Status counts:{" "}
          {Object.entries(byStatus)
            .map(([s, n]) => `${s} × ${n}`)
            .join(" · ")}
        </p>
      )}
    </section>
  );
}
