import type { Transaction } from "../types";

export interface PipelineCounts {
  accepted: number; // 202s sent from this dashboard (session)
  rejected: number; // non-2xx responses (session)
  queued: number; // accepted, not yet consumed by the worker
  processing: number;
  processed: number;
  failed: number;
}

export type Stage = "api" | "kafka" | "worker" | "db";

interface Props {
  counts: PipelineCounts;
  inFlight: Transaction[];
  delaySeconds: number;
  now: number;
  lastActivity: Record<Stage, number>;
}

const ACTIVE_MS = 1200;

function StageNode(props: {
  title: string;
  subtitle: string;
  value: number;
  valueLabel: string;
  extra?: string;
  active: boolean;
  tone: string;
}) {
  return (
    <div className={`stage ${props.tone} ${props.active ? "active" : ""}`}>
      <div className="stage-title">{props.title}</div>
      <div className="stage-sub">{props.subtitle}</div>
      <div className="stage-value">{props.value}</div>
      <div className="stage-value-label">{props.valueLabel}</div>
      {props.extra && <div className="stage-extra">{props.extra}</div>}
    </div>
  );
}

function Connector({ active, label }: { active: boolean; label: string }) {
  return (
    <div className={`connector ${active ? "flowing" : ""}`}>
      <span className="connector-line" />
      <span className="connector-label">{label}</span>
    </div>
  );
}

export function PipelineView({ counts, inFlight, delaySeconds, now, lastActivity }: Props) {
  const active = (s: Stage) => now - lastActivity[s] < ACTIVE_MS;
  return (
    <section className="card pipeline">
      <div className="card-head">
        <h2>Live pipeline</h2>
        <span className="hint">nodes light up as events pass through</span>
      </div>
      <div className="stages">
        <StageNode
          title="Webhook API"
          subtitle="FastAPI · validate → 202"
          value={counts.accepted}
          valueLabel="accepted (202)"
          extra={counts.rejected ? `${counts.rejected} rejected` : undefined}
          active={active("api")}
          tone="tone-api"
        />
        <Connector active={active("api") || active("kafka")} label="produce acks=all" />
        <StageNode
          title="Kafka"
          subtitle="transactions.received"
          value={counts.queued}
          valueLabel="waiting to be consumed"
          active={active("kafka")}
          tone="tone-kafka"
        />
        <Connector active={active("kafka") || active("worker")} label="consume" />
        <StageNode
          title="Worker"
          subtitle={`idempotent insert · wait ${delaySeconds} s`}
          value={counts.processing}
          valueLabel="PROCESSING"
          active={active("worker")}
          tone="tone-worker"
        />
        <Connector active={active("worker") || active("db")} label="UPDATE" />
        <StageNode
          title="Postgres"
          subtitle="source of truth"
          value={counts.processed}
          valueLabel="PROCESSED"
          extra={counts.failed ? `${counts.failed} FAILED` : undefined}
          active={active("db")}
          tone="tone-db"
        />
      </div>

      <div className="inflight">
        <div className="inflight-head">
          In flight <span className="hint">({inFlight.length})</span>
        </div>
        {inFlight.length === 0 ? (
          <p className="empty">No transaction is waiting. Send one to watch its 30 s countdown.</p>
        ) : (
          <ul>
            {inFlight.slice(0, 8).map((t) => {
              const elapsed = (now - Date.parse(t.created_at)) / 1000;
              const pct = Math.min(100, Math.max(0, (elapsed / delaySeconds) * 100));
              const left = Math.max(0, delaySeconds - elapsed);
              return (
                <li key={t.transaction_id}>
                  <span className="mono id">{t.transaction_id}</span>
                  <span className="bar">
                    <span className="bar-fill" style={{ width: `${pct}%` }} />
                  </span>
                  <span className="mono left">{left > 0 ? `${left.toFixed(1)} s` : "finishing…"}</span>
                </li>
              );
            })}
            {inFlight.length > 8 && <li className="more">+ {inFlight.length - 8} more</li>}
          </ul>
        )}
      </div>
    </section>
  );
}
