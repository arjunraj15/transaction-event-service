// Hooks for state and for the polling side effect.
import { useEffect, useState } from "react";
// API calls: readiness probe and raw Prometheus metrics text.
import { getMetricsText, getReadiness } from "../api";
// Extracts average webhook latency from Prometheus text.
import { parseWebhookLatency } from "../lib/stats";
// Shared types (type-only, erased at build).
import type { Meta, Readiness, StreamState } from "../types";

// Values the parent passes in.
interface Props {
  // Service metadata (environment, processing delay); null until loaded.
  meta: Meta | null;
  // State of the SSE connection to the API.
  stream: StreamState;
  // Whether the API reports its event bus (Postgres LISTEN) as connected.
  busConnected: boolean;
  // Optional sign-out handler; the button only renders when provided.
  onSignOut?: () => void;
}

// How often to re-poll health and metrics.
const POLL_MS = 5000;

// Small status badge: green when ok, red when not, grey when unknown (null).
function Pill({ ok, label, title }: { ok: boolean | null; label: string; title?: string }) {
  // Pick the CSS class from the tri-state value.
  const cls = ok === null ? "pill pill-unknown" : ok ? "pill pill-ok" : "pill pill-bad";
  return (
    <span className={cls} title={title}>
      {/* Coloured status dot */}
      <span className="dot" />
      {label}
    </span>
  );
}

// Top bar showing branding, dependency health, live-stream state and key metadata.
export function HealthBar({ meta, stream, busConnected, onSignOut }: Props) {
  // Latest readiness response; null if never fetched or the call failed.
  const [ready, setReady] = useState<Readiness | null>(null);
  // Whether the API answered at all; null before the first poll.
  const [reachable, setReachable] = useState<boolean | null>(null);
  // Average server-side webhook latency derived from metrics.
  const [latency, setLatency] = useState<{ avgMs: number; count: number } | null>(null);

  // Start polling on mount and stop on unmount.
  useEffect(() => {
    // Guards against setting state after unmount.
    let cancelled = false;
    // One poll cycle: fetch readiness and metrics together.
    async function poll() {
      const [r, metrics] = await Promise.all([getReadiness(), getMetricsText()]);
      // Ignore responses that arrive after the component unmounted.
      if (cancelled) return;
      setReady(r);
      // A null readiness means the API was unreachable.
      setReachable(r !== null);
      // Only update latency when metrics were actually returned.
      if (metrics) setLatency(parseWebhookLatency(metrics));
    }
    // Poll immediately (void: intentionally not awaited).
    void poll();
    // Then keep polling on an interval.
    const timer = setInterval(poll, POLL_MS);
    // Cleanup: flag cancellation and stop the timer.
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
    // Empty deps: set up once.
  }, []);

  return (
    <header className="health-bar">
      {/* Logo and title */}
      <div className="brand">
        <span className="logo">◆</span>
        <div>
          <h1>Transaction Event Service</h1>
          <p className="subtitle">Demo dashboard · send webhooks and watch them flow through the pipeline</p>
        </div>
      </div>
      {/* Status pills and metadata */}
      <div className="health-items">
        {/* API reachability */}
        <Pill ok={reachable} label="API" title="GET /health/ready reachable" />
        {/* Database check; unknown until readiness is loaded */}
        <Pill ok={ready ? ready.checks.database : null} label="Postgres" />
        {/* Kafka check; unknown until readiness is loaded */}
        <Pill ok={ready ? ready.checks.kafka : null} label="Kafka" />
        <Pill
          // Live: reflect bus state; connecting: unknown; otherwise: bad.
          ok={stream === "live" ? busConnected : stream === "connecting" ? null : false}
          // Label mirrors the stream state.
          label={stream === "live" ? "Live events" : stream === "connecting" ? "Connecting…" : "Reconnecting…"}
          title="SSE stream from /v1/events/stream (worker → Postgres NOTIFY → API)"
        />
        {/* Average server-side acknowledgement time, or a dash if unavailable */}
        <span className="meta-item" title="Average server-side time for 202 webhook responses (one API process)">
          server ack <strong>{latency ? `${latency.avgMs.toFixed(1)} ms` : "—"}</strong>
        </span>
        {/* Configured simulated processing delay */}
        <span className="meta-item">
          delay <strong>{meta ? `${meta.processing_delay_seconds} s` : "—"}</strong>
        </span>
        {/* Deployment environment name */}
        <span className="meta-item">
          env <strong>{meta?.environment ?? "—"}</strong>
        </span>
        {/* Sign-out button, shown only when a handler is supplied */}
        {onSignOut && (
          <button className="signout" onClick={onSignOut}>
            Sign out
          </button>
        )}
      </div>
    </header>
  );
}
