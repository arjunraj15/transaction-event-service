import type { Meta, Readiness, RequestResult, Transaction, WebhookPayload } from "./types";

// The dashboard is its own static site: in production VITE_API_BASE is the API's URL (CORS allow-listed).
// In development it is empty and Vite proxies the calls to the API (same origin).
// Render's `fromService` provides a bare host (e.g. api.example.com), so default the scheme to https.
const rawBase: string = (import.meta.env.VITE_API_BASE ?? "").trim().replace(/\/$/, "");
export const API_BASE: string = rawBase && !rawBase.includes("://") ? `https://${rawBase}` : rawBase;
export const apiUrl = (path: string): string => `${API_BASE}${path}`;

// Admin key for the dashboard-only endpoints. Kept in sessionStorage (cleared when the tab closes) and
// only ever sent in the X-Admin-Key header, never in a URL.
const ADMIN_KEY_STORAGE = "txn-dashboard-admin-key";
export const getAdminKey = (): string | null => sessionStorage.getItem(ADMIN_KEY_STORAGE);
export const setAdminKey = (key: string | null): void =>
  key ? sessionStorage.setItem(ADMIN_KEY_STORAGE, key) : sessionStorage.removeItem(ADMIN_KEY_STORAGE);
export const adminHeaders = (): Record<string, string> => {
  const key = getAdminKey();
  return key ? { "X-Admin-Key": key } : {};
};

export class UnauthorizedError extends Error {}

let nextId = 1;

/** Performs a request and records status, wall-clock latency and the parsed body. Never throws. */
export async function timedRequest(
  label: string,
  method: "GET" | "POST",
  path: string,
  options: { json?: unknown; rawBody?: string; transactionId?: string } = {},
): Promise<RequestResult> {
  const started = performance.now();
  const init: RequestInit = { method, headers: {} };
  if (options.json !== undefined || options.rawBody !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = options.rawBody ?? JSON.stringify(options.json);
  }
  let status = 0;
  let body: unknown;
  try {
    const res = await fetch(apiUrl(path), init);
    status = res.status;
    const text = await res.text();
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = text;
    }
  } catch (err) {
    body = { error: String(err) };
  }
  return {
    id: nextId++,
    label,
    method,
    path,
    status,
    ms: performance.now() - started,
    body,
    transactionId: options.transactionId,
    at: new Date(),
  };
}

export const postWebhook = (label: string, payload: WebhookPayload) =>
  timedRequest(label, "POST", "/v1/webhooks/transactions", {
    json: payload,
    transactionId: payload.transaction_id,
  });

async function getJson<T>(path: string, headers: Record<string, string> = {}): Promise<T> {
  const res = await fetch(apiUrl(path), { headers });
  if (res.status === 401) throw new UnauthorizedError(`${path} -> HTTP 401`);
  if (!res.ok) throw new Error(`${path} -> HTTP ${res.status}`);
  return (await res.json()) as T;
}

export const listTransactions = (limit = 100) =>
  getJson<Transaction[]>(`/v1/transactions?limit=${limit}`, adminHeaders());
export const getMeta = () => getJson<Meta>("/v1/meta");

/** True if the key is accepted by the admin endpoints (checked with a 1-row list). */
export async function verifyAdminKey(key: string): Promise<boolean> {
  const res = await fetch(apiUrl("/v1/transactions?limit=1"), { headers: { "X-Admin-Key": key } });
  return res.ok;
}

export async function getReadiness(): Promise<Readiness | null> {
  try {
    const res = await fetch(apiUrl("/health/ready"));
    return (await res.json()) as Readiness; // body is meaningful for both 200 and 503
  } catch {
    return null;
  }
}

export async function getMetricsText(): Promise<string | null> {
  try {
    const res = await fetch(apiUrl("/metrics"));
    return res.ok ? await res.text() : null;
  } catch {
    return null;
  }
}

export function newTransactionId(prefix = "txn_ui"): string {
  const random =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID().replace(/-/g, "").slice(0, 12)
      : Math.random().toString(36).slice(2, 14);
  return `${prefix}_${random}`;
}
