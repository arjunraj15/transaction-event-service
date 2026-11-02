import { describe, expect, it } from "vitest";
import { histogram, parseWebhookLatency, percentile, runWithConcurrency, summarize } from "./stats";

describe("percentile / summarize", () => {
  it("uses nearest rank", () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(percentile(sorted, 50)).toBe(50);
    expect(percentile(sorted, 95)).toBe(95);
    expect(percentile(sorted, 100)).toBe(100);
    expect(percentile([], 50)).toBe(0);
  });

  it("summarizes unsorted input", () => {
    expect(summarize([30, 10, 20])).toEqual({ count: 3, min: 10, p50: 20, p95: 30, max: 30, mean: 20 });
    expect(summarize([])).toBeNull();
  });
});

describe("histogram", () => {
  it("puts every value in exactly one bucket, max in the last", () => {
    const buckets = histogram([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 5);
    expect(buckets).toHaveLength(5);
    expect(buckets.reduce((n, b) => n + b.count, 0)).toBe(11);
    expect(buckets[4].count).toBe(3); // 8, 9, 10
  });

  it("handles identical values and empty input", () => {
    expect(histogram([5, 5, 5])).toEqual([{ from: 5, to: 5, count: 3 }]);
    expect(histogram([])).toEqual([]);
  });
});

describe("runWithConcurrency", () => {
  it("never exceeds the limit and keeps result order", async () => {
    let inFlight = 0;
    let peak = 0;
    const tasks = Array.from({ length: 12 }, (_, i) => async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 5 + (i % 3)));
      inFlight--;
      return i;
    });
    const seen: number[] = [];
    const results = await runWithConcurrency(tasks, 3, (r) => seen.push(r));
    expect(results).toEqual(Array.from({ length: 12 }, (_, i) => i));
    expect(peak).toBe(3);
    expect(seen.sort((a, b) => a - b)).toEqual(results);
  });
});

describe("parseWebhookLatency", () => {
  it("reads the 202 webhook sum/count from Prometheus text", () => {
    const text = [
      'http_request_duration_seconds_count{method="POST",route="/v1/webhooks/transactions",status="503"} 2.0',
      'http_request_duration_seconds_sum{method="POST",route="/v1/webhooks/transactions",status="202"} 0.45',
      'http_request_duration_seconds_count{method="POST",route="/v1/webhooks/transactions",status="202"} 50.0',
    ].join("\n");
    const parsed = parseWebhookLatency(text);
    expect(parsed?.count).toBe(50);
    expect(parsed?.avgMs).toBeCloseTo(9, 5);
  });

  it("returns null without 202 samples", () => {
    expect(parseWebhookLatency("# nothing here")).toBeNull();
  });
});
