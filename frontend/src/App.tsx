import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { apiUrl, getAdminKey, getMeta, setAdminKey, UnauthorizedError } from "./api";
import { ActionPanel, type BurstRun } from "./components/ActionPanel";
import { HealthBar } from "./components/HealthBar";
import { InspectDrawer } from "./components/InspectDrawer";
import { LiveLog } from "./components/LiveLog";
import { LoadResults } from "./components/LoadResults";
import { LoginScreen } from "./components/LoginScreen";
import { PipelineView, type Stage } from "./components/PipelineView";
import { ResponseView } from "./components/ResponseView";
import { TransactionTable } from "./components/TransactionTable";
import { useEventStream } from "./hooks/useEventStream";
import { useLog } from "./hooks/useLog";
import { useNow } from "./hooks/useNow";
import { useTransactions } from "./hooks/useTransactions";
import type { Meta, RequestResult, ServerEvent } from "./types";

const RECONCILE_MS = 15_000; // safety net: re-sync from REST in case an event was missed
const WEBHOOK_PATH = "/v1/webhooks/transactions";

/** Loads public metadata, then shows the admin-key login if the API requires one. */
export function App() {
  const [meta, setMeta] = useState<Meta | null>(null);
  const [metaError, setMetaError] = useState(false);
  const [authorized, setAuthorized] = useState(() => getAdminKey() !== null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    getMeta()
      .then(setMeta)
      .catch(() => setMetaError(true));
  }, []);

  const signOut = useCallback((message: string | null = null) => {
    setAdminKey(null);
    setAuthorized(false);
    setNotice(message);
  }, []);

  if (!meta) {
    return (
      <div className="login">
        <p className="hint">{metaError ? `Could not reach the API at ${apiUrl("/v1/meta")}.` : "Connecting to the API…"}</p>
      </div>
    );
  }
  if (meta.admin_key_required && !authorized) {
    return (
      <>
        {notice && <p className="login-notice">{notice}</p>}
        <LoginScreen
          onAuthenticated={(key) => {
            setAdminKey(key);
            setAuthorized(true);
            setNotice(null);
          }}
        />
      </>
    );
  }
  return (
    <Dashboard
      meta={meta}
      onSignOut={meta.admin_key_required ? () => signOut() : undefined}
      onUnauthorized={() => signOut("Your admin key is no longer accepted. Please sign in again.")}
    />
  );
}

interface DashboardProps {
  meta: Meta;
  onSignOut?: () => void;
  onUnauthorized: () => void;
}

function Dashboard({ meta, onSignOut, onUnauthorized }: DashboardProps) {
  const [results, setResults] = useState<RequestResult[]>([]);
  const [burst, setBurst] = useState<BurstRun | null>(null);
  const [inspect, setInspect] = useState<string | null>(null);
  const [busConnected, setBusConnected] = useState(false);
  const [sessionIds, setSessionIds] = useState<Set<string>>(new Set());
  const [accepted, setAccepted] = useState(0);
  const [rejected, setRejected] = useState(0);
  const activity = useRef<Record<Stage, number>>({ api: 0, kafka: 0, worker: 0, db: 0 });

  const { rows, queued, reseed: reseedRaw, applyEvent, markAccepted } = useTransactions();
  const log = useLog();
  const now = useNow(250);
  const delay = meta.processing_delay_seconds;
  const unauthorized = useRef(onUnauthorized);
  unauthorized.current = onUnauthorized;
  const reseed = useCallback(
    () =>
      reseedRaw().catch((err) => {
        if (err instanceof UnauthorizedError) unauthorized.current();
      }),
    [reseedRaw],
  );

  const pulse = (...stages: Stage[]) => {
    for (const s of stages) activity.current[s] = Date.now();
  };

  useEffect(() => {
    void reseed();
    const timer = setInterval(() => void reseed(), RECONCILE_MS);
    return () => clearInterval(timer);
  }, [reseed]);

  const onEvent = useCallback(
    (event: ServerEvent) => {
      applyEvent(event);
      log.addServerEvent(event);
      if (event.type === "ingested" || event.type === "resumed") pulse("kafka", "worker");
      else if (event.type === "duplicate_ignored") pulse("kafka", "worker");
      else pulse("worker", "db");
    },
    [applyEvent, log.addServerEvent],
  );

  const onConnected = useCallback(
    (bus: boolean) => {
      setBusConnected(bus);
      log.add({
        source: "system",
        level: bus ? "ok" : "warn",
        text: bus
          ? "system  connected to live event stream (worker → Postgres NOTIFY → API → SSE)"
          : "system  stream open, but the API's Postgres listener is still connecting",
      });
      void reseed();
    },
    [reseed, log.add],
  );

  const stream = useEventStream(onEvent, onConnected, () => unauthorized.current(), true);

  const onResult = useCallback(
    (r: RequestResult) => {
      setResults((prev) => [...prev.slice(-199), r]);
      log.addRequest(r);
      if (r.method !== "POST" || r.path !== WEBHOOK_PATH) return;
      pulse("api");
      if (r.status === 202 && r.transactionId) {
        setAccepted((n) => n + 1);
        setSessionIds((prev) => (prev.has(r.transactionId!) ? prev : new Set(prev).add(r.transactionId!)));
        markAccepted(r.transactionId);
        pulse("kafka");
      } else {
        setRejected((n) => n + 1);
      }
    },
    [markAccepted, log.addRequest],
  );

  const inFlight = useMemo(
    () => rows.filter((r) => r.status === "PROCESSING").sort((a, b) => a.created_at.localeCompare(b.created_at)),
    [rows],
  );
  const counts = useMemo(
    () => ({
      accepted,
      rejected,
      queued: queued.size,
      processing: inFlight.length,
      processed: rows.filter((r) => r.status === "PROCESSED").length,
      failed: rows.filter((r) => r.status === "FAILED").length,
    }),
    [accepted, rejected, queued, inFlight, rows],
  );

  return (
    <div className="app">
      <HealthBar meta={meta} stream={stream} busConnected={busConnected} onSignOut={onSignOut} />
      <main className="grid">
        <div className="col-left">
          <ActionPanel onResult={onResult} onBurst={setBurst} />
          {burst && <LoadResults run={burst} />}
          <ResponseView results={results} />
        </div>
        <div className="col-right">
          <PipelineView
            counts={counts}
            inFlight={inFlight}
            delaySeconds={delay}
            now={now}
            lastActivity={activity.current}
          />
          <div className="split">
            <TransactionTable rows={rows} now={now} delaySeconds={delay} sessionIds={sessionIds} onInspect={setInspect} />
            <LiveLog entries={log.entries} onClear={log.clear} />
          </div>
        </div>
      </main>
      <footer className="footer">
        Optional demo for the backend assessment, deployed as its own static site. It calls the graded API
        unchanged, plus admin endpoints (<code>/v1/transactions</code>, <code>/v1/events/stream</code>) that need the
        admin key.{" "}
        <a href={apiUrl("/docs")} target="_blank" rel="noreferrer">
          OpenAPI docs
        </a>
      </footer>
      {inspect && <InspectDrawer transactionId={inspect} onClose={() => setInspect(null)} />}
    </div>
  );
}
