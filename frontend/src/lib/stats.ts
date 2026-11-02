export interface LatencySummary {
  count: number;
  min: number;
  p50: number;
  p95: number;
  max: number;
  mean: number;
}

/** Nearest-rank percentile on an ascending-sorted array. */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))];
}

export function summarize(values: number[]): LatencySummary | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
    min: sorted[0],
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    max: sorted[sorted.length - 1],
    mean: sorted.reduce((sum, v) => sum + v, 0) / sorted.length,
  };
}

export interface Bucket {
  from: number;
  to: number;
  count: number;
}

/** Equal-width buckets between min and max (a single bucket when all values are equal). */
export function histogram(values: number[], bucketCount = 20): Bucket[] {
  if (values.length === 0) return [];
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) return [{ from: min, to: max, count: values.length }];
  const width = (max - min) / bucketCount;
  const buckets: Bucket[] = Array.from({ length: bucketCount }, (_, i) => ({
    from: min + i * width,
    to: min + (i + 1) * width,
    count: 0,
  }));
  for (const v of values) {
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
  const results: T[] = new Array(tasks.length);
  let next = 0;
  async function lane(): Promise<void> {
    while (next < tasks.length) {
      const index = next++;
      results[index] = await tasks[index]();
      onResult?.(results[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, tasks.length)) }, lane));
  return results;
}

/**
 * Average server-side webhook latency for 202 responses from Prometheus text (`/metrics`).
 * Each API process keeps its own counters, so this reflects whichever process answered.
 */
export function parseWebhookLatency(metrics: string): { avgMs: number; count: number } | null {
  const line = (kind: "sum" | "count") =>
    new RegExp(
      `^http_request_duration_seconds_${kind}\\{[^}]*route="/v1/webhooks/transactions"[^}]*status="202"[^}]*\\} ([0-9.eE+-]+)$`,
      "m",
    );
  const sum = metrics.match(line("sum"));
  const count = metrics.match(line("count"));
  if (!sum || !count) return null;
  const n = Number(count[1]);
  return n > 0 ? { avgMs: (Number(sum[1]) / n) * 1000, count: n } : null;
}
