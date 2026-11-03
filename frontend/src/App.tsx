// React primitives: callbacks/memo for stable identities, refs for mutable non-render state.
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
// API helpers: URL builder, admin-key storage, metadata fetch, and the 401 error type.
import { apiUrl, getAdminKey, getMeta, setAdminKey, UnauthorizedError } from "./api";
// Panel with buttons that send test requests; BurstRun describes a load-test result.
import { ActionPanel, type BurstRun } from "./components/ActionPanel";
// Top bar showing API/stream health.
import { HealthBar } from "./components/HealthBar";
// Drawer showing the details of one transaction.
import { InspectDrawer } from "./components/InspectDrawer";
// Scrolling log of client and server activity.
import { LiveLog } from "./components/LiveLog";
// Summary of the last burst/load run.
import { LoadResults } from "./components/LoadResults";
// Admin-key sign-in form.
import { LoginScreen } from "./components/LoginScreen";
// Animated diagram of the pipeline; Stage names its steps.
import { PipelineView, type Stage } from "./components/PipelineView";
// Shows recent HTTP responses.
import { ResponseView } from "./components/ResponseView";
// Table of transactions.
import { TransactionTable } from "./components/TransactionTable";
// Data hooks that feed the dashboard.
import { useEventStream } from "./hooks/useEventStream";
import { useLog } from "./hooks/useLog";
import { useNow } from "./hooks/useNow";
import { useTransactions } from "./hooks/useTransactions";
// Shared types.
import type { Meta, RequestResult, ServerEvent } from "./types";

const RECONCILE_MS = 15_000; // safety net: re-sync from REST in case an event was missed
// Webhook endpoint path; used to recognise ingestion calls among all requests.
const WEBHOOK_PATH = "/v1/webhooks/transactions";

/** Loads public metadata, then shows the admin-key login if the API requires one. */
export function App() {
  // Server metadata; null until loaded.
  const [meta, setMeta] = useState<Meta | null>(null);
  // Set when the metadata request fails, to show an error instead of a spinner.
  const [metaError, setMetaError] = useState(false);
  // Lazy init: already authorized if a key is in sessionStorage.
  const [authorized, setAuthorized] = useState(() => getAdminKey() !== null);
  // Message shown above the login form (e.g. after a rejected key).
  const [notice, setNotice] = useState<string | null>(null);

  // Fetch metadata once on mount.
  useEffect(() => {
    getMeta()
      .then(setMeta)
      .catch(() => setMetaError(true));
  }, []);

  // Clears credentials and returns to the login screen, optionally with a message.
  const signOut = useCallback((message: string | null = null) => {
    // Forget the stored key.
    setAdminKey(null);
    setAuthorized(false);
    setNotice(message);
  }, []);

  // Until metadata arrives we don't know whether a login is needed.
  if (!meta) {
    return (
      <div className="login">
        {/* Either the failure message or a loading hint. */}
        <p className="hint">{metaError ? `Could not reach the API at ${apiUrl("/v1/meta")}.` : "Connecting to the API…"}</p>
      </div>
    );
  }
  // The API wants an admin key and we don't have an accepted one yet: show the login.
  if (meta.admin_key_required && !authorized) {
    return (
      <>
        {/* Explains why the user was sent back to login, if applicable. */}
        {notice && <p className="login-notice">{notice}</p>}
        <LoginScreen
          // Persist the verified key and enter the dashboard.
          onAuthenticated={(key) => {
            setAdminKey(key);
            setAuthorized(true);
            setNotice(null);
          }}
        />
      </>
    );
  }
  // Authorized (or no key needed): show the dashboard.
  return (
    <Dashboard
      meta={meta}
      // Only offer sign-out when a key is in use.
      onSignOut={meta.admin_key_required ? () => signOut() : undefined}
      // A 401 mid-session means the key was revoked or changed.
      onUnauthorized={() => signOut("Your admin key is no longer accepted. Please sign in again.")}
    />
  );
}

// Props for the dashboard screen.
interface DashboardProps {
  // Public server metadata.
  meta: Meta;
  // Sign-out handler; absent when no admin key is required.
  onSignOut?: () => void;
  // Called when the API rejects the admin key.
  onUnauthorized: () => void;
}

// The main authenticated view: wires data sources to the display components.
function Dashboard({ meta, onSignOut, onUnauthorized }: DashboardProps) {
  // Recent HTTP results shown in the response panel.
  const [results, setResults] = useState<RequestResult[]>([]);
  // Latest burst/load-test run, if any.
  const [burst, setBurst] = useState<BurstRun | null>(null);
  // Transaction id currently open in the inspect drawer.
  const [inspect, setInspect] = useState<string | null>(null);
  // Whether the API's Postgres listener (event bus) is connected.
  const [busConnected, setBusConnected] = useState(false);
  // Ids of transactions submitted from this browser session (highlighted in the table).
  const [sessionIds, setSessionIds] = useState<Set<string>>(new Set());
  // Counts of webhook requests accepted / rejected during this session.
  const [accepted, setAccepted] = useState(0);
  const [rejected, setRejected] = useState(0);
  // Last-activity timestamps per pipeline stage; a ref so pulsing doesn't force renders by itself.
  const activity = useRef<Record<Stage, number>>({ api: 0, kafka: 0, worker: 0, db: 0 });

  // Transaction state and the operations that update it.
  const { rows, queued, totals, reseed: reseedRaw, applyEvent, markAccepted } = useTransactions();
  // Log store for the live log panel.
  const log = useLog();
  // Ticking clock (250 ms) that drives countdowns and highlights.
  const now = useNow(250);
  // Simulated worker delay, used for progress/countdown display.
  const delay = meta.processing_delay_seconds;
  // Hold the latest onUnauthorized in a ref so callbacks below stay stable.
  const unauthorized = useRef(onUnauthorized);
  unauthorized.current = onUnauthorized;
  // Reseed wrapper that signs out on 401 and swallows nothing else (other errors are handled inside).
  const reseed = useCallback(
    () =>
      reseedRaw().catch((err) => {
        if (err instanceof UnauthorizedError) unauthorized.current();
      }),
    [reseedRaw],
  );

  // Mark the given pipeline stages as active right now.
  const pulse = (...stages: Stage[]) => {
    for (const s of stages) activity.current[s] = Date.now();
  };

  // Initial load plus periodic reconciliation with the REST API.
  useEffect(() => {
    void reseed();
    // Periodic re-sync repairs any state drift caused by missed events.
    const timer = setInterval(() => void reseed(), RECONCILE_MS);
    // Stop the timer on unmount.
    return () => clearInterval(timer);
  }, [reseed]);

  // Handles each live event from the server stream.
  const onEvent = useCallback(
    (event: ServerEvent) => {
      // Update the transaction rows.
      applyEvent(event);
      // Append to the live log.
      log.addServerEvent(event);
      // Light up the pipeline stages the event implies: intake touches Kafka and the worker...
      if (event.type === "ingested" || event.type === "resumed") pulse("kafka", "worker");
      // ...a duplicate is consumed from Kafka and checked by the worker, but writes nothing...
      else if (event.type === "duplicate_ignored") pulse("kafka", "worker");
      // ...and completion/failure involves the worker and the database.
      else pulse("worker", "db");
    },
    [applyEvent, log.addServerEvent],
  );

  // Handles the stream's (re)connect handshake.
  const onConnected = useCallback(
    (bus: boolean) => {
      setBusConnected(bus);
      // Tell the user which state the stream is in.
      log.add({
        source: "system",
        level: bus ? "ok" : "warn",
        text: bus
          ? "system  connected to live event stream (worker → Postgres NOTIFY → API → SSE)"
          : "system  stream open, but the API's Postgres listener is still connecting",
      });
      // Re-sync, since events may have been missed while disconnected.
      void reseed();
    },
    [reseed, log.add],
  );

  // Subscribe to the live stream; the 4th argument enables it.
  const stream = useEventStream(onEvent, onConnected, () => unauthorized.current(), true);

  // Handles the result of any HTTP request the action panel makes.
  const onResult = useCallback(
    (r: RequestResult) => {
      // Keep only the latest 200 results (199 old + the new one).
      setResults((prev) => [...prev.slice(-199), r]);
      // Mirror it into the log.
      log.addRequest(r);
      // Only webhook POSTs affect the counters and pipeline below.
      if (r.method !== "POST" || r.path !== WEBHOOK_PATH) return;
      // The API handled the request.
      pulse("api");
      // 202 with an id means the transaction was accepted and queued.
      if (r.status === 202 && r.transactionId) {
        setAccepted((n) => n + 1);
        // Remember the id for highlighting; reuse the old Set if already present to skip a re-render.
        setSessionIds((prev) => (prev.has(r.transactionId!) ? prev : new Set(prev).add(r.transactionId!)));
        // Show it as queued until the worker reports it.
        markAccepted(r.transactionId);
        // The API published it to Kafka.
        pulse("kafka");
      } else {
        // Anything else (validation error, auth failure, network error) counts as rejected.
        setRejected((n) => n + 1);
      }
    },
    [markAccepted, log.addRequest],
  );

  // Transactions still being processed, oldest first.
  const inFlight = useMemo(
    () => rows.filter((r) => r.status === "PROCESSING").sort((a, b) => a.created_at.localeCompare(b.created_at)),
    [rows],
  );
  // Aggregate numbers for the pipeline view.
  const counts = useMemo(
    () => ({
      accepted,
      rejected,
      // Accepted by the API but not yet seen by the worker.
      queued: queued.size,
      processing: inFlight.length,
      // Server totals cover the whole table; local rows keep the count live between reseeds.
      processed: Math.max(totals.PROCESSED ?? 0, rows.filter((r) => r.status === "PROCESSED").length),
      failed: Math.max(totals.FAILED ?? 0, rows.filter((r) => r.status === "FAILED").length),
    }),
    [accepted, rejected, queued, inFlight, rows, totals],
  );

  return (
    <div className="app">
      {/* Top bar: environment, stream status and optional sign-out. */}
      <HealthBar meta={meta} stream={stream} busConnected={busConnected} onSignOut={onSignOut} />
      <main className="grid">
        {/* Left column: actions and their outcomes. */}
        <div className="col-left">
          {/* Buttons that send requests; reports results and burst runs upward. */}
          <ActionPanel onResult={onResult} onBurst={setBurst} />
          {/* Only shown once a burst has been run. */}
          {burst && <LoadResults run={burst} />}
          {/* Recent request/response details. */}
          <ResponseView results={results} />
        </div>
        {/* Right column: pipeline visualisation, table and log. */}
        <div className="col-right">
          {/* Live diagram of where transactions are in the pipeline. */}
          <PipelineView
            counts={counts}
            inFlight={inFlight}
            delaySeconds={delay}
            now={now}
            lastActivity={activity.current}
          />
          <div className="split">
            {/* Transaction list; clicking a row opens the inspect drawer. */}
            <TransactionTable rows={rows} totals={totals} now={now} delaySeconds={delay} sessionIds={sessionIds} onInspect={setInspect} />
            {/* Combined client/server log with a clear button. */}
            <LiveLog entries={log.entries} onClear={log.clear} />
          </div>
        </div>
      </main>
      <footer className="footer">
        {/* Explains what this demo is and which endpoints need the admin key. */}
        Optional demo for the backend assessment, deployed as its own static site. It calls the graded API
        unchanged, plus admin endpoints (<code>/v1/transactions</code>, <code>/v1/events/stream</code>) that need the
        admin key.{" "}
        {/* Link to the API's interactive docs, opened in a new tab without leaking the opener. */}
        <a href={apiUrl("/docs")} target="_blank" rel="noreferrer">
          OpenAPI docs
        </a>
      </footer>
      {/* Drawer rendered only while a transaction is selected. */}
      {inspect && <InspectDrawer transactionId={inspect} onClose={() => setInspect(null)} />}
    </div>
  );
}
