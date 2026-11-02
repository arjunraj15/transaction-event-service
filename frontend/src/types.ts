export type TxnStatus = "PROCESSING" | "PROCESSED" | "FAILED";

/** Shape of GET /v1/transactions and GET /v1/transactions/{id} rows (the brief's contract). */
export interface Transaction {
  transaction_id: string;
  source_account: string;
  destination_account: string;
  amount: number;
  currency: string;
  status: TxnStatus;
  created_at: string;
  processed_at: string | null;
}

export type ServerEventType =
  | "ingested"
  | "resumed"
  | "duplicate_ignored"
  | "processed"
  | "dead_lettered"
  | "invalid_message";

/** Live event pushed by GET /v1/events/stream (emitted by the worker via Postgres NOTIFY). */
export interface ServerEvent {
  type: ServerEventType;
  at: string;
  transaction_id?: string;
  status?: TxnStatus;
  amount?: string;
  currency?: string;
  created_at?: string;
  processed_at?: string;
  duration_s?: number;
  detail?: string;
}

export interface WebhookPayload {
  transaction_id: string;
  source_account: string;
  destination_account: string;
  amount: number;
  currency: string;
}

/** One HTTP call made by the dashboard, with what came back. */
export interface RequestResult {
  id: number;
  label: string;
  method: "GET" | "POST";
  path: string;
  status: number; // 0 = network error
  ms: number;
  body: unknown;
  transactionId?: string;
  at: Date;
}

export type LogLevel = "ok" | "info" | "warn" | "error";

export interface LogEntry {
  id: number;
  at: Date;
  source: "client" | "server" | "system";
  level: LogLevel;
  transactionId?: string;
  text: string;
}

export interface Meta {
  environment: string;
  processing_delay_seconds: number;
  signature_required: boolean;
  admin_key_required: boolean;
}

export interface Readiness {
  status: "READY" | "NOT_READY";
  checks: { database: boolean; kafka: boolean };
}

export type StreamState = "connecting" | "live" | "reconnecting";
