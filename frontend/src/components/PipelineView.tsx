// Transaction row type, used for the in-flight list.
import type { Transaction } from "../types";

// Counters displayed on the pipeline nodes.
export interface PipelineCounts {
  accepted: number; // 202s sent from this dashboard (session)
  rejected: number; // non-2xx responses (session)
  queued: number; // accepted, not yet consumed by the worker
  // Rows the worker is currently handling.
  processing: number;
  // Rows that completed successfully.
  processed: number;
  // Rows that ended in failure.
  failed: number;
}

// The four pipeline stages, used as keys for activity tracking.
export type Stage = "api" | "kafka" | "worker" | "db";

// Inputs from the parent.
interface Props {
  // Per-stage counters.
  counts: PipelineCounts;
  // Transactions still being processed (shown with countdowns).
  inFlight: Transaction[];
  // Simulated processing delay, used to compute progress.
  delaySeconds: number;
  // Current timestamp in ms, ticked by the parent so countdowns animate.
  now: number;
  // Epoch ms of the most recent event seen at each stage.
  lastActivity: Record<Stage, number>;
}

// A stage counts as "active" (glowing) for this long after its last event.
const ACTIVE_MS = 1200;

// One box in the pipeline diagram.
function StageNode(props: {
  // Stage name.
  title: string;
  // Short technology/behaviour description.
  subtitle: string;
  // Main big number.
  value: number;
  // Caption under the number.
  valueLabel: string;
  // Optional extra line (e.g. rejected/failed counts).
  extra?: string;
  // Whether to highlight because of recent activity.
  active: boolean;
  // CSS class that sets the node's colour.
  tone: string;
}) {
  return (
    // Combine tone and active classes
    <div className={`stage ${props.tone} ${props.active ? "active" : ""}`}>
      <div className="stage-title">{props.title}</div>
      <div className="stage-sub">{props.subtitle}</div>
      <div className="stage-value">{props.value}</div>
      <div className="stage-value-label">{props.valueLabel}</div>
      {/* Render the extra line only when provided */}
      {props.extra && <div className="stage-extra">{props.extra}</div>}
    </div>
  );
}

// Arrow between two stages; animates while events are flowing.
function Connector({ active, label }: { active: boolean; label: string }) {
  return (
    <div className={`connector ${active ? "flowing" : ""}`}>
      <span className="connector-line" />
      <span className="connector-label">{label}</span>
    </div>
  );
}

// Visual pipeline: API -> Kafka -> Worker -> Postgres, plus in-flight countdowns.
export function PipelineView({ counts, inFlight, delaySeconds, now, lastActivity }: Props) {
  // True if the stage saw an event within the last ACTIVE_MS.
  const active = (s: Stage) => now - lastActivity[s] < ACTIVE_MS;
  return (
    <section className="card pipeline">
      {/* Header */}
      <div className="card-head">
        <h2>Live pipeline</h2>
        <span className="hint">nodes light up as events pass through</span>
      </div>
      {/* The four stages and the connectors between them */}
      <div className="stages">
        {/* Stage 1: webhook API */}
        <StageNode
          title="Webhook API"
          subtitle="FastAPI · validate → 202"
          value={counts.accepted}
          valueLabel="accepted (202)"
          // Show rejected count only when non-zero.
          extra={counts.rejected ? `${counts.rejected} rejected` : undefined}
          active={active("api")}
          tone="tone-api"
        />
        {/* API to Kafka: flows when either end is active */}
        <Connector active={active("api") || active("kafka")} label="produce acks=all" />
        {/* Stage 2: Kafka topic backlog */}
        <StageNode
          title="Kafka"
          subtitle="transactions.received"
          value={counts.queued}
          valueLabel="waiting to be consumed"
          active={active("kafka")}
          tone="tone-kafka"
        />
        {/* Kafka to worker */}
        <Connector active={active("kafka") || active("worker")} label="consume" />
        {/* Stage 3: worker, subtitle includes the configured delay */}
        <StageNode
          title="Worker"
          subtitle={`idempotent insert · wait ${delaySeconds} s`}
          value={counts.processing}
          valueLabel="PROCESSING"
          active={active("worker")}
          tone="tone-worker"
        />
        {/* Worker to database */}
        <Connector active={active("worker") || active("db")} label="UPDATE" />
        {/* Stage 4: Postgres final state */}
        <StageNode
          title="Postgres"
          subtitle="source of truth"
          value={counts.processed}
          valueLabel="PROCESSED"
          // Show failed count only when non-zero.
          extra={counts.failed ? `${counts.failed} FAILED` : undefined}
          active={active("db")}
          tone="tone-db"
        />
      </div>

      {/* Transactions currently waiting out the delay */}
      <div className="inflight">
        <div className="inflight-head">
          In flight <span className="hint">({inFlight.length})</span>
        </div>
        {/* Empty message vs. countdown list */}
        {inFlight.length === 0 ? (
          <p className="empty">No transaction is waiting. Send one to watch its 30 s countdown.</p>
        ) : (
          <ul>
            {/* Cap the list at 8 rows to keep the card compact */}
            {inFlight.slice(0, 8).map((t) => {
              // Seconds since the transaction was created.
              const elapsed = (now - Date.parse(t.created_at)) / 1000;
              // Progress percentage clamped to 0..100.
              const pct = Math.min(100, Math.max(0, (elapsed / delaySeconds) * 100));
              // Seconds remaining, never negative.
              const left = Math.max(0, delaySeconds - elapsed);
              return (
                <li key={t.transaction_id}>
                  <span className="mono id">{t.transaction_id}</span>
                  {/* Progress bar track and fill */}
                  <span className="bar">
                    <span className="bar-fill" style={{ width: `${pct}%` }} />
                  </span>
                  {/* Countdown text; "finishing…" once the delay has elapsed */}
                  <span className="mono left">{left > 0 ? `${left.toFixed(1)} s` : "finishing…"}</span>
                </li>
              );
            })}
            {/* Overflow indicator when more than 8 are in flight */}
            {inFlight.length > 8 && <li className="more">+ {inFlight.length - 8} more</li>}
          </ul>
        )}
      </div>
    </section>
  );
}
