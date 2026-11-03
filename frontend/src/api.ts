// Shared types for requests and responses.
import type { Meta, Readiness, RequestResult, Transaction, WebhookPayload } from "./types";

// The dashboard is its own static site: in production VITE_API_BASE is the API's URL (CORS allow-listed).
// In development it is empty and Vite proxies the calls to the API (same origin).
// A bare host (e.g. api.example.com) is accepted too and defaults to https.
// Normalize the env value: default to empty, trim whitespace, and strip one trailing slash.
const rawBase: string = (import.meta.env.VITE_API_BASE ?? "").trim().replace(/\/$/, "");
// Add https:// when a bare host was given; leave full URLs and the empty (proxy) case untouched.
export const API_BASE: string = rawBase && !rawBase.includes("://") ? `https://${rawBase}` : rawBase;
// Build an absolute (or proxy-relative) URL for an API path.
export const apiUrl = (path: string): string => `${API_BASE}${path}`;

// Admin key for the dashboard-only endpoints. Kept in sessionStorage (cleared when the tab closes) and
// only ever sent in the X-Admin-Key header, never in a URL.
// sessionStorage key under which the admin key is stored.
const ADMIN_KEY_STORAGE = "txn-dashboard-admin-key";
// Read the stored key, or null when not signed in.
export const getAdminKey = (): string | null => sessionStorage.getItem(ADMIN_KEY_STORAGE);
// Store the key, or clear it when given null (sign-out).
export const setAdminKey = (key: string | null): void =>
  key ? sessionStorage.setItem(ADMIN_KEY_STORAGE, key) : sessionStorage.removeItem(ADMIN_KEY_STORAGE);
// Headers to spread into admin requests; empty when there is no key so public calls stay header-free.
export const adminHeaders = (): Record<string, string> => {
  const key = getAdminKey();
  return key ? { "X-Admin-Key": key } : {};
};

// Distinct error type so callers can tell "401, sign in again" apart from other failures.
export class UnauthorizedError extends Error {}

// Counter giving each recorded request a unique id.
let nextId = 1;

/** Performs a request and records status, wall-clock latency and the parsed body. Never throws. */
export async function timedRequest(
  label: string,
  method: "GET" | "POST",
  path: string,
  options: { json?: unknown; rawBody?: string; transactionId?: string } = {},
): Promise<RequestResult> {
  // High-resolution start time for latency measurement.
  const started = performance.now();
  // Base fetch options; no body by default.
  const init: RequestInit = { method, headers: {} };
  // Only attach a body when one was supplied (rawBody lets callers send deliberately malformed JSON).
  if (options.json !== undefined || options.rawBody !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = options.rawBody ?? JSON.stringify(options.json);
  }
  // 0 means "no HTTP response" (network error) and is what RequestResult documents.
  let status = 0;
  // Parsed response body (or error info).
  let body: unknown;
  try {
    const res = await fetch(apiUrl(path), init);
    status = res.status;
    // Read as text first so a non-JSON body does not lose its content.
    const text = await res.text();
    try {
      // Empty bodies (e.g. 204) become null.
      body = text ? JSON.parse(text) : null;
    } catch {
      // Not JSON: keep the raw text for display.
      body = text;
    }
  } catch (err) {
    // Network failure/CORS: record the error instead of throwing, so the UI can show it.
    body = { error: String(err) };
  }
  return {
    id: nextId++,
    label,
    method,
    path,
    status,
    // Elapsed time includes reading the body.
    ms: performance.now() - started,
    body,
    transactionId: options.transactionId,
    at: new Date(),
  };
}

// Convenience wrapper: POST a transaction webhook and tag the result with its transaction id.
export const postWebhook = (label: string, payload: WebhookPayload) =>
  timedRequest(label, "POST", "/v1/webhooks/transactions", {
    json: payload,
    transactionId: payload.transaction_id,
  });

// Typed GET helper that, unlike timedRequest, throws on failure.
async function getJson<T>(path: string, headers: Record<string, string> = {}): Promise<T> {
  const res = await fetch(apiUrl(path), { headers });
  // 401 gets its own error type so the app can send the user back to the login screen.
  if (res.status === 401) throw new UnauthorizedError(`${path} -> HTTP 401`);
  // Any other non-2xx is a generic failure.
  if (!res.ok) throw new Error(`${path} -> HTTP ${res.status}`);
  // Trust the API contract for the response shape.
  return (await res.json()) as T;
}

// Fetch the most recent transactions (admin endpoint, so the key header is attached).
export const listTransactions = (limit = 100, offset = 0, statuses: string[] = []) => {
  const qs = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  for (const s of statuses) qs.append("status", s);
  return getJson<Transaction[]>(`/v1/transactions?${qs}`, adminHeaders());
};
// Fetch whole-table totals per status (e.g. {PROCESSED: 5230}), unlike the capped list above.
export const getStats = () => getJson<Record<string, number>>("/v1/transactions/stats", adminHeaders());
// Fetch public server metadata (no auth needed).
export const getMeta = () => getJson<Meta>("/v1/meta");

/** True if the key is accepted by the admin endpoints (checked with a 1-row list). */
export async function verifyAdminKey(key: string): Promise<boolean> {
  // Use the candidate key directly (it is not stored yet) and keep the probe cheap with limit=1.
  const res = await fetch(apiUrl("/v1/transactions?limit=1"), { headers: { "X-Admin-Key": key } });
  return res.ok;
}

// Check API readiness; returns null if the API is unreachable.
export async function getReadiness(): Promise<Readiness | null> {
  try {
    const res = await fetch(apiUrl("/health/ready"));
    return (await res.json()) as Readiness; // body is meaningful for both 200 and 503
  } catch {
    // Network error or non-JSON body: treat as unknown.
    return null;
  }
}

// Fetch raw Prometheus metrics text, or null when unavailable.
export async function getMetricsText(): Promise<string | null> {
  try {
    const res = await fetch(apiUrl("/metrics"));
    return res.ok ? await res.text() : null;
  } catch {
    return null;
  }
}

// Generate a unique transaction id for demo requests, e.g. txn_ui_1a2b3c4d5e6f.
export function newTransactionId(prefix = "txn_ui"): string {
  // Prefer a cryptographically random UUID; fall back to Math.random in environments without it.
  const random =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID().replace(/-/g, "").slice(0, 12)
      : Math.random().toString(36).slice(2, 14);
  return `${prefix}_${random}`;
}
