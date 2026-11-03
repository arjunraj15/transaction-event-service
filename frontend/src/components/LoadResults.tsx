// Stats helpers: latency histogram buckets and percentile summary.
import { histogram, summarize } from "../lib/stats";
// Burst run type defined by the panel that produces it.
import type { BurstRun } from "./ActionPanel";

// SVG drawing width in viewBox units.
const W = 360;
// SVG bar-area height in viewBox units (extra room below is for axis labels).
const H = 90;

/** Latency summary and histogram for the last load burst (browser-measured, includes network). */
export function LoadResults({ run }: { run: BurstRun }) {
  // Per-request latencies in ms.
  const latencies = run.results.map((r) => r.ms);
  // Percentiles (p50/p95/max); presumably null when there is no data.
  const summary = summarize(latencies);
  // Split latencies into 24 bins for the chart.
  const buckets = histogram(latencies, 24);
  // Tallest bar, floored at 1 to avoid dividing by zero.
  const peak = Math.max(1, ...buckets.map((b) => b.count));
  // Count responses per HTTP status.
  const byStatus = run.results.reduce<Record<string, number>>((acc, r) => {
    // Status 0 (network failure) is labelled "ERR".
    const key = String(r.status || "ERR");
    // Increment, starting from 0.
    acc[key] = (acc[key] ?? 0) + 1;
    return acc;
  }, {});
  // Seconds elapsed; uses "now" while the burst is still running.
  const elapsed = ((run.finishedAt ?? Date.now()) - run.startedAt) / 1000;
  // Number of successful 202 Accepted responses.
  const ok = byStatus["202"] ?? 0;

  return (
    <section className="card load-results">
      {/* Header with run label, progress and elapsed time */}
      <div className="card-head">
        <h2>Load burst</h2>
        <span className="hint">
          {run.label} · {run.results.length}/{run.total} · {elapsed.toFixed(1)} s
          {run.finishedAt === null && " · running…"}
        </span>
      </div>
      {/* Row of headline stats */}
      <div className="stat-row">
        {/* Green only if every response so far was 202 */}
        <div className={`stat ${ok === run.results.length && ok > 0 ? "good" : "bad"}`}>
          <span className="stat-value">
            {ok}/{run.results.length}
          </span>
          <span className="stat-label">202 Accepted</span>
        </div>
        {/* p50, p95 and max latency tiles */}
        {(["p50", "p95", "max"] as const).map((k) => (
          // Percentiles under 500 ms are marked good; max is never highlighted
          <div key={k} className={`stat ${k !== "max" && summary && summary[k] < 500 ? "good" : ""}`}>
            <span className="stat-value">{summary ? `${summary[k].toFixed(0)} ms` : "—"}</span>
            <span className="stat-label">{k}</span>
          </div>
        ))}
      </div>
      {/* Histogram only when there is data */}
      {buckets.length > 0 && (
        <svg className="histogram" viewBox={`0 0 ${W} ${H + 18}`} role="img" aria-label="latency histogram">
          {buckets.map((b, i) => {
            // Width allotted to each bar.
            const bw = W / buckets.length;
            // Bar height scaled relative to the tallest bar.
            const h = (b.count / peak) * H;
            return (
              // Bars grow upward from the baseline; 1px gap each side, minimum 1px wide
              <rect key={i} x={i * bw + 1} y={H - h} width={Math.max(1, bw - 2)} height={h} rx={2}>
                {/* Tooltip with range and count */}
                <title>
                  {b.from.toFixed(0)}–{b.to.toFixed(0)} ms: {b.count}
                </title>
              </rect>
            );
          })}
          {/* Left axis label: smallest latency */}
          <text x={0} y={H + 14}>
            {buckets[0].from.toFixed(0)} ms
          </text>
          {/* Right axis label: largest latency, right-aligned */}
          <text x={W} y={H + 14} textAnchor="end">
            {buckets[buckets.length - 1].to.toFixed(0)} ms
          </text>
        </svg>
      )}
      {/* Show a status breakdown only if something other than 202 occurred */}
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
