// Lifecycle states of a transaction as stored by the backend.
export type TxnStatus = "PROCESSING" | "PROCESSED" | "FAILED";

/** Shape of GET /v1/transactions and GET /v1/transactions/{id} rows (the brief's contract). */
export interface Transaction {
  // Client-supplied unique id (also the idempotency key).
  transaction_id: string;
  // Account the money leaves.
  source_account: string;
  // Account the money arrives in.
  destination_account: string;
  // Transfer amount (numeric here; the API sends decimals).
  amount: number;
  // Currency code, e.g. "USD".
  currency: string;
  // Where the transaction is in its lifecycle.
  status: TxnStatus;
  // ISO timestamp of when the worker first stored the row.
  created_at: string;
  // ISO timestamp of completion; null while still PROCESSING (or failed).
  processed_at: string | null;
}

// Event kinds the worker publishes over the live stream.
export type ServerEventType =
  // New transaction row written.
  | "ingested"
  // Message redelivered while the row was still PROCESSING.
  | "resumed"
  // Message for an already-stored transaction was skipped.
  | "duplicate_ignored"
  // Processing finished successfully.
  | "processed"
  // Gave up and sent to the dead-letter queue.
  | "dead_lettered"
  // Message could not be parsed/validated.
  | "invalid_message";

/** Live event pushed by GET /v1/events/stream (emitted by the worker via Postgres NOTIFY). */
export interface ServerEvent {
  // What happened.
  type: ServerEventType;
  // ISO timestamp of when the event occurred.
  at: string;
  // The transaction concerned (absent for invalid messages).
  transaction_id?: string;
  // Status at the time of the event, when relevant.
  status?: TxnStatus;
  // Amount as a decimal string (preserves precision over JSON).
  amount?: string;
  // Currency code.
  currency?: string;
  // Row creation time, if known.
  created_at?: string;
  // Completion time, if known.
  processed_at?: string;
  // Seconds the processing took (for "processed" events).
  duration_s?: number;
  // Free-text reason, used for failures and dead-letters.
  detail?: string;
}

// Body sent to POST /v1/webhooks/transactions.
export interface WebhookPayload {
  transaction_id: string;
  source_account: string;
  destination_account: string;
  amount: number;
  currency: string;
}

/** One HTTP call made by the dashboard, with what came back. */
export interface RequestResult {
  // Monotonic local id, used as a React key.
  id: number;
  // Human-readable name of the action that triggered the call.
  label: string;
  // HTTP verb used.
  method: "GET" | "POST";
  // Request path (without the API base).
  path: string;
  status: number; // 0 = network error
  // Round-trip latency in milliseconds.
  ms: number;
  // Parsed JSON body, or raw text / error object when parsing or the request failed.
  body: unknown;
  // Transaction this call concerned, if any.
  transactionId?: string;
  // When the request completed.
  at: Date;
}

// Severity used to colour log lines.
export type LogLevel = "ok" | "info" | "warn" | "error";

// One line in the live log panel.
export interface LogEntry {
  // Monotonic local id, used as a React key.
  id: number;
  // When the logged thing happened.
  at: Date;
  // Origin of the line: this browser, the backend stream, or the dashboard itself.
  source: "client" | "server" | "system";
  // Severity for styling.
  level: LogLevel;
  // Related transaction, enabling filtering/inspection.
  transactionId?: string;
  // The rendered message.
  text: string;
}

// Public server metadata from GET /v1/meta.
export interface Meta {
  // Deployment environment name.
  environment: string;
  // Artificial worker delay, used for countdown displays.
  processing_delay_seconds: number;
  // Whether webhooks must be HMAC-signed.
  signature_required: boolean;
  // Whether the dashboard must show the admin-key login.
  admin_key_required: boolean;
}

// Body of GET /health/ready.
export interface Readiness {
  // Overall verdict.
  status: "READY" | "NOT_READY";
  // Per-dependency health.
  checks: { database: boolean; kafka: boolean };
}

// Connection state of the live event stream, shown in the health bar.
export type StreamState = "connecting" | "live" | "reconnecting";
