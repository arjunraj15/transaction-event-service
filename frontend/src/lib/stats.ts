// Summary statistics for a set of latency samples (all in the same unit, typically ms).
export interface LatencySummary {
  // Number of samples.
  count: number;
  // Fastest sample.
  min: number;
  // Median.
  p50: number;
  // 95th percentile: the tail latency most users stay under.
  p95: number;
  // Slowest sample.
  max: number;
  // Arithmetic average.
  mean: number;
}

/** Nearest-rank percentile on an ascending-sorted array. */
export function percentile(sorted: number[], p: number): number {
  // No data: return 0 rather than NaN/undefined so callers can render it safely.
  if (sorted.length === 0) return 0;
  // Nearest-rank method: the 1-based rank covering p percent of the samples.
  const rank = Math.ceil((p / 100) * sorted.length);
  // Convert to a 0-based index and clamp into bounds (handles p=0 and p=100).
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))];
}

// Returns null for an empty input so the UI can show "no data" instead of zeros.
export function summarize(values: number[]): LatencySummary | null {
  if (values.length === 0) return null;
  // Copy before sorting so the caller's array is not mutated.
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
    // After sorting, min and max are the ends.
    min: sorted[0],
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    max: sorted[sorted.length - 1],
    // Sum divided by count.
    mean: sorted.reduce((sum, v) => sum + v, 0) / sorted.length,
  };
}

// One histogram bar: the half-open value range and how many samples fall in it.
export interface Bucket {
  // Lower bound of the bucket.
  from: number;
  // Upper bound of the bucket.
  to: number;
  // Samples that landed in this bucket.
  count: number;
}

/** Equal-width buckets between min and max (a single bucket when all values are equal). */
export function histogram(values: number[], bucketCount = 20): Bucket[] {
  // Nothing to chart.
  if (values.length === 0) return [];
  // Range of the data defines the bucket span.
  const min = Math.min(...values);
  const max = Math.max(...values);
  // Zero-width range would divide by zero below, so collapse to one bucket.
  if (min === max) return [{ from: min, to: max, count: values.length }];
  // Width of each equal bucket.
  const width = (max - min) / bucketCount;
  // Pre-build empty buckets covering [min, max].
  const buckets: Bucket[] = Array.from({ length: bucketCount }, (_, i) => ({
    from: min + i * width,
    to: min + (i + 1) * width,
    count: 0,
  }));
  // Place each value in its bucket.
  for (const v of values) {
    // Clamp so the maximum value (which computes to index bucketCount) lands in the last bucket.
    buckets[Math.min(bucketCount - 1, Math.floor((v - min) / width))].count++;
  }
  return buckets;
}

/** Runs async tasks with at most `limit` in flight, reporting each result as it completes. */
export async function runWithConcurrency<T>(
  tasks: Array<() => Promise<T>>,
  limit: number,
  onResult?: (result: T, index: number) => void,
): Promise<T[]> {
  // Preallocated so each result is stored at its task's index, preserving input order.
  const results: T[] = new Array(tasks.length);
  // Shared cursor: the next task index to claim (safe because JS is single-threaded between awaits).
  let next = 0;
  // A "lane" is a worker that keeps pulling tasks until none are left.
  async function lane(): Promise<void> {
    while (next < tasks.length) {
      // Claim the next task before awaiting so no two lanes run the same one.
      const index = next++;
      results[index] = await tasks[index]();
      // Notify the caller as soon as each task finishes (for live progress).
      onResult?.(results[index], index);
    }
  }
  // Start up to `limit` lanes (at least 1, never more than there are tasks) and wait for all to drain.
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, tasks.length)) }, lane));
  return results;
}

/**
 * Average server-side webhook latency for 202 responses from Prometheus text (`/metrics`).
 * Each API process keeps its own counters, so this reflects whichever process answered.
 */
export function parseWebhookLatency(metrics: string): { avgMs: number; count: number } | null {
  // Builds a regex matching the histogram's _sum or _count line for the accepted-webhook route.
  const line = (kind: "sum" | "count") =>
    new RegExp(
      // Capture the numeric sample value; labels may appear in any surrounding order.
      `^http_request_duration_seconds_${kind}\\{[^}]*route="/v1/webhooks/transactions"[^}]*status="202"[^}]*\\} ([0-9.eE+-]+)$`,
      // "m" makes ^ and $ match per line of the metrics text.
      "m",
    );
  // Total seconds spent and number of observed requests.
  const sum = metrics.match(line("sum"));
  const count = metrics.match(line("count"));
  // Metrics not present yet (no accepted webhooks): nothing to report.
  if (!sum || !count) return null;
  const n = Number(count[1]);
  // Average = sum / count, converted from seconds to ms; guard against division by zero.
  return n > 0 ? { avgMs: (Number(sum[1]) / n) * 1000, count: n } : null;
}
