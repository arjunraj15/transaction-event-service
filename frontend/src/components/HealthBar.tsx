import { useEffect, useState } from "react";
import { getMetricsText, getReadiness } from "../api";
import { parseWebhookLatency } from "../lib/stats";
import type { Meta, Readiness, StreamState } from "../types";

interface Props {
  meta: Meta | null;
  stream: StreamState;
  busConnected: boolean;
  onSignOut?: () => void;
}

const POLL_MS = 5000;

function Pill({ ok, label, title }: { ok: boolean | null; label: string; title?: string }) {
  const cls = ok === null ? "pill pill-unknown" : ok ? "pill pill-ok" : "pill pill-bad";
  return (
    <span className={cls} title={title}>
      <span className="dot" />
      {label}
    </span>
  );
}

export function HealthBar({ meta, stream, busConnected, onSignOut }: Props) {
  const [ready, setReady] = useState<Readiness | null>(null);
  const [reachable, setReachable] = useState<boolean | null>(null);
  const [latency, setLatency] = useState<{ avgMs: number; count: number } | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      const [r, metrics] = await Promise.all([getReadiness(), getMetricsText()]);
      if (cancelled) return;
      setReady(r);
      setReachable(r !== null);
      if (metrics) setLatency(parseWebhookLatency(metrics));
    }
    void poll();
    const timer = setInterval(poll, POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  return (
    <header className="health-bar">
      <div className="brand">
        <span className="logo">◆</span>
        <div>
          <h1>Transaction Event Service</h1>
          <p className="subtitle">Demo dashboard · send webhooks and watch them flow through the pipeline</p>
        </div>
      </div>
      <div className="health-items">
        <Pill ok={reachable} label="API" title="GET /health/ready reachable" />
        <Pill ok={ready ? ready.checks.database : null} label="Postgres" />
        <Pill ok={ready ? ready.checks.kafka : null} label="Kafka" />
        <Pill
          ok={stream === "live" ? busConnected : stream === "connecting" ? null : false}
          label={stream === "live" ? "Live events" : stream === "connecting" ? "Connecting…" : "Reconnecting…"}
          title="SSE stream from /v1/events/stream (worker → Postgres NOTIFY → API)"
        />
        <span className="meta-item" title="Average server-side time for 202 webhook responses (one API process)">
          server ack <strong>{latency ? `${latency.avgMs.toFixed(1)} ms` : "—"}</strong>
        </span>
        <span className="meta-item">
          delay <strong>{meta ? `${meta.processing_delay_seconds} s` : "—"}</strong>
        </span>
        <span className="meta-item">
          env <strong>{meta?.environment ?? "—"}</strong>
        </span>
        {onSignOut && (
          <button className="signout" onClick={onSignOut}>
            Sign out
          </button>
        )}
      </div>
    </header>
  );
}
