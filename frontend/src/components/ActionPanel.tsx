// React state hook for the form fields and busy/progress flags.
import { useState } from "react";
// API helpers: id generator, typed webhook POST, and the generic timed request used for negative tests.
import { newTransactionId, postWebhook, timedRequest } from "../api";
// Runs a list of async tasks with a bounded number in flight (used by the load burst).
import { runWithConcurrency } from "../lib/stats";
// Type-only imports so they are erased at build time.
import type { RequestResult, WebhookPayload } from "../types";

// Snapshot of one load burst, shared with the parent so LoadResults can render it live.
export interface BurstRun {
  // Human-readable description shown in the results card.
  label: string;
  // Number of requests the burst was asked to send.
  total: number;
  // Maximum number of requests in flight at once.
  concurrency: number;
  // Results collected so far (grows as requests finish).
  results: RequestResult[];
  // Epoch ms when the burst started.
  startedAt: number;
  // Epoch ms when it finished; null while still running.
  finishedAt: number | null;
}

// Callbacks the parent supplies to receive output from this panel.
interface Props {
  // Called once per completed request so it can be logged/tabled.
  onResult: (result: RequestResult) => void;
  // Called repeatedly with the latest burst snapshot.
  onBurst: (run: BurstRun) => void;
}

// The four demo scenarios, one per tab.
type Tab = "single" | "duplicates" | "burst" | "negative";

// Tab metadata: id, button label, and the assignment criterion each tab demonstrates.
const TABS: Array<{ id: Tab; label: string; criterion: string }> = [
  { id: "single", label: "Single", criterion: "Criterion 1: one webhook, processed after ~30 s" },
  { id: "duplicates", label: "Duplicates", criterion: "Criterion 2: same transaction_id N times → processed once" },
  { id: "burst", label: "Load burst", criterion: "Criterion 3: fast acknowledgements under load" },
  { id: "negative", label: "Negative", criterion: "Criterion 4: errors handled gracefully" },
];

// Round to an integer and keep within [lo, hi]; NaN/0 falls back to lo so bad input can't break a run.
const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, Math.round(v) || lo));

// Panel with tabs for sending single, duplicate, burst and invalid webhooks.
export function ActionPanel({ onResult, onBurst }: Props) {
  // Currently selected scenario tab.
  const [tab, setTab] = useState<Tab>("single");
  // Editable transaction id; lazily initialised with a fresh random id.
  const [txnId, setTxnId] = useState(() => newTransactionId());
  // Form fields for the webhook body, pre-filled with sample values.
  const [source, setSource] = useState("acc_user_789");
  const [destination, setDestination] = useState("acc_merchant_456");
  const [amount, setAmount] = useState(1500);
  const [currency, setCurrency] = useState("INR");
  // Number of identical requests for the duplicates scenario.
  const [copies, setCopies] = useState(5);
  // Burst size and parallelism.
  const [burstTotal, setBurstTotal] = useState(50);
  const [burstConcurrency, setBurstConcurrency] = useState(10);
  // True while a scenario runs; disables buttons to prevent overlapping runs.
  const [busy, setBusy] = useState(false);
  // Status line shown under the form during long scenarios.
  const [progress, setProgress] = useState<string | null>(null);

  // Build a webhook body from the current form values for the given transaction id.
  const payload = (id: string): WebhookPayload => ({
    transaction_id: id,
    source_account: source,
    destination_account: destination,
    amount,
    currency,
  });

  // Wrap a scenario so busy/progress are always reset, even if it throws.
  async function guarded(work: () => Promise<void>) {
    // Disable the action buttons.
    setBusy(true);
    try {
      // Run the scenario.
      await work();
    } finally {
      // Always re-enable the UI and clear the progress text.
      setBusy(false);
      setProgress(null);
    }
  }

  // Criterion 1: send one webhook.
  const sendSingle = () =>
    guarded(async () => {
      // Post and report the result to the parent.
      onResult(await postWebhook("single", payload(txnId)));
      // Rotate the id so the next click is a new transaction, not a duplicate.
      setTxnId(newTransactionId());
    });

  // Criterion 2: send the same id several times in parallel to prove idempotency.
  const sendDuplicates = () =>
    guarded(async () => {
      // Freeze the id so every copy shares it.
      const id = txnId;
      // Keep the copy count within 2..20.
      const n = clamp(copies, 2, 20);
      // Tell the user what is happening.
      setProgress(`sending ${n} copies of ${id} in parallel…`);
      // Fire all copies at once and wait for every response.
      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => postWebhook(`duplicate ${i + 1}/${n}`, payload(id))),
      );
      // Report each response individually.
      results.forEach(onResult);
      // Fresh id for the next run.
      setTxnId(newTransactionId());
    });

  // Criterion 3: send many unique webhooks with bounded concurrency.
  const sendBurst = () =>
    guarded(async () => {
      // Clamp inputs to the limits documented in the UI.
      const total = clamp(burstTotal, 1, 200);
      const concurrency = clamp(burstConcurrency, 1, 20);
      // Mutable run record that accumulates results.
      const run: BurstRun = {
        label: `${total} unique @ ${concurrency} concurrent`,
        total,
        concurrency,
        results: [],
        startedAt: Date.now(),
        finishedAt: null,
      };
      // Publish the initial (empty) state; spread so React sees a new object.
      onBurst({ ...run });
      // Shared prefix so all burst ids are unique but recognisable.
      const prefix = newTransactionId("txn_burst");
      // Lazy tasks (functions) so the runner controls when each request starts.
      const tasks = Array.from({ length: total }, (_, i) => () => postWebhook(`burst ${i + 1}/${total}`, payload(`${prefix}_${i}`)));
      // Execute with limited parallelism; the callback fires as each request completes.
      await runWithConcurrency(tasks, concurrency, (result) => {
        // Record the result in the run.
        run.results.push(result);
        // Also surface it in the global log/table.
        onResult(result);
        // Update progress text.
        setProgress(`${run.results.length} / ${total} sent`);
        // Publish a copy (new array) so the results card re-renders.
        onBurst({ ...run, results: [...run.results] });
      });
      // Final snapshot with the finish time set.
      onBurst({ ...run, results: [...run.results], finishedAt: Date.now() });
    });

  // Negative test: schema-invalid body should be rejected with 422.
  const sendInvalid = () =>
    guarded(async () => {
      onResult(
        // Bad id format, negative amount and invalid currency all violate validation.
        await timedRequest("invalid payload (expect 422)", "POST", "/v1/webhooks/transactions", {
          json: { transaction_id: "bad id with spaces", amount: -1, currency: "RUPEE" },
        }),
      );
    });

  // Negative test: unparseable JSON should also be rejected with 422.
  const sendMalformed = () =>
    guarded(async () => {
      onResult(
        // rawBody bypasses JSON.stringify so the broken text is sent as-is.
        await timedRequest("malformed JSON (expect 422)", "POST", "/v1/webhooks/transactions", {
          rawBody: "{not json",
        }),
      );
    });

  // Negative test: looking up a transaction that doesn't exist should return 404.
  const lookupUnknown = () =>
    guarded(async () => {
      // Random id that is guaranteed not to exist.
      const id = newTransactionId("txn_missing");
      // transactionId lets the log associate this request with the id.
      onResult(await timedRequest("unknown id (expect 404)", "GET", `/v1/transactions/${id}`, { transactionId: id }));
    });

  // Metadata for the active tab (non-null: tab is always one of TABS).
  const current = TABS.find((t) => t.id === tab)!;

  return (
    <section className="card action-panel">
      {/* Card header with title and hint */}
      <div className="card-head">
        <h2>Send webhooks</h2>
        <span className="hint">replaces the curl commands</span>
      </div>
      {/* Tab strip, one button per scenario */}
      <div className="tabs" role="tablist">
        {TABS.map((t) => (
          <button
            // Stable key for list rendering.
            key={t.id}
            // Accessibility: announce as a tab and its selected state.
            role="tab"
            aria-selected={tab === t.id}
            // Highlight the active tab.
            className={tab === t.id ? "tab active" : "tab"}
            // Switch scenario on click.
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>
      {/* Which acceptance criterion this tab demonstrates */}
      <p className="criterion">{current.criterion}</p>

      {/* Webhook form, shared by the single and duplicates tabs */}
      {(tab === "single" || tab === "duplicates") && (
        <div className="form-grid">
          {/* Transaction id spans two columns and has a regenerate button */}
          <label className="span-2">
            transaction_id
            <div className="with-button">
              {/* Controlled input; spellcheck off since ids aren't words */}
              <input value={txnId} onChange={(e) => setTxnId(e.target.value)} spellCheck={false} />
              {/* type="button" avoids form submit; generates a new id */}
              <button type="button" className="ghost" title="Generate a new id" onClick={() => setTxnId(newTransactionId())}>
                ↻
              </button>
            </div>
          </label>
          {/* Source account */}
          <label>
            source_account
            <input value={source} onChange={(e) => setSource(e.target.value)} />
          </label>
          {/* Destination account */}
          <label>
            destination_account
            <input value={destination} onChange={(e) => setDestination(e.target.value)} />
          </label>
          {/* Amount; converted to a number, min matches server validation (> 0) */}
          <label>
            amount
            <input type="number" min={0.01} step="0.01" value={amount} onChange={(e) => setAmount(Number(e.target.value))} />
          </label>
          {/* ISO currency code, forced to upper case, max 3 letters */}
          <label>
            currency
            <input value={currency} maxLength={3} onChange={(e) => setCurrency(e.target.value.toUpperCase())} />
          </label>
        </div>
      )}

      {/* Single tab: one send button */}
      {tab === "single" && (
        <button className="primary" disabled={busy} onClick={sendSingle}>
          POST webhook
        </button>
      )}

      {/* Duplicates tab: copy count and send button */}
      {tab === "duplicates" && (
        <div className="row">
          <label className="inline">
            copies
            {/* Raw value is clamped to 2..20 when sending */}
            <input type="number" min={2} max={20} value={copies} onChange={(e) => setCopies(Number(e.target.value))} />
          </label>
          <button className="primary" disabled={busy} onClick={sendDuplicates}>
            {/* Label shows the effective (clamped) copy count */}
            POST {clamp(copies, 2, 20)}× same id
          </button>
        </div>
      )}

      {/* Burst tab: inputs plus explanatory note */}
      {tab === "burst" && (
        <>
          <div className="row">
            <label className="inline">
              total
              {/* Number of unique requests, up to 200 */}
              <input type="number" min={1} max={200} value={burstTotal} onChange={(e) => setBurstTotal(Number(e.target.value))} />
            </label>
            <label className="inline">
              concurrency
              {/* Parallel requests in flight, up to 20 */}
              <input
                type="number"
                min={1}
                max={20}
                value={burstConcurrency}
                onChange={(e) => setBurstConcurrency(Number(e.target.value))}
              />
            </label>
            <button className="primary" disabled={busy} onClick={sendBurst}>
              Run burst
            </button>
          </div>
          {/* Caveat: browser-side timings include network latency */}
          <p className="note">
            Unique ids, up to 200 requests and 20 in flight. Latency is measured in the browser, so it includes your
            network; the server-side time is shown in the header. Use k6 for a real load test.
          </p>
        </>
      )}

      {/* Negative tab: one button per error case */}
      {tab === "negative" && (
        <div className="button-stack">
          {/* Schema violation */}
          <button disabled={busy} onClick={sendInvalid}>
            Invalid payload → 422
          </button>
          {/* Broken JSON */}
          <button disabled={busy} onClick={sendMalformed}>
            Malformed JSON → 422
          </button>
          {/* Missing resource */}
          <button disabled={busy} onClick={lookupUnknown}>
            Unknown transaction → 404
          </button>
        </div>
      )}

      {/* Live progress text, only while a scenario reports it */}
      {progress && <p className="progress-text">{progress}</p>}
    </section>
  );
}
