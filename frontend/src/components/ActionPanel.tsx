import { useState } from "react";
import { newTransactionId, postWebhook, timedRequest } from "../api";
import { runWithConcurrency } from "../lib/stats";
import type { RequestResult, WebhookPayload } from "../types";

export interface BurstRun {
  label: string;
  total: number;
  concurrency: number;
  results: RequestResult[];
  startedAt: number;
  finishedAt: number | null;
}

interface Props {
  onResult: (result: RequestResult) => void;
  onBurst: (run: BurstRun) => void;
}

type Tab = "single" | "duplicates" | "burst" | "negative";

const TABS: Array<{ id: Tab; label: string; criterion: string }> = [
  { id: "single", label: "Single", criterion: "Criterion 1: one webhook, processed after ~30 s" },
  { id: "duplicates", label: "Duplicates", criterion: "Criterion 2: same transaction_id N times → processed once" },
  { id: "burst", label: "Load burst", criterion: "Criterion 3: fast acknowledgements under load" },
  { id: "negative", label: "Negative", criterion: "Criterion 4: errors handled gracefully" },
];

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, Math.round(v) || lo));

export function ActionPanel({ onResult, onBurst }: Props) {
  const [tab, setTab] = useState<Tab>("single");
  const [txnId, setTxnId] = useState(() => newTransactionId());
  const [source, setSource] = useState("acc_user_789");
  const [destination, setDestination] = useState("acc_merchant_456");
  const [amount, setAmount] = useState(1500);
  const [currency, setCurrency] = useState("INR");
  const [copies, setCopies] = useState(5);
  const [burstTotal, setBurstTotal] = useState(50);
  const [burstConcurrency, setBurstConcurrency] = useState(10);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState<string | null>(null);

  const payload = (id: string): WebhookPayload => ({
    transaction_id: id,
    source_account: source,
    destination_account: destination,
    amount,
    currency,
  });

  async function guarded(work: () => Promise<void>) {
    setBusy(true);
    try {
      await work();
    } finally {
      setBusy(false);
      setProgress(null);
    }
  }

  const sendSingle = () =>
    guarded(async () => {
      onResult(await postWebhook("single", payload(txnId)));
      setTxnId(newTransactionId());
    });

  const sendDuplicates = () =>
    guarded(async () => {
      const id = txnId;
      const n = clamp(copies, 2, 20);
      setProgress(`sending ${n} copies of ${id} in parallel…`);
      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => postWebhook(`duplicate ${i + 1}/${n}`, payload(id))),
      );
      results.forEach(onResult);
      setTxnId(newTransactionId());
    });

  const sendBurst = () =>
    guarded(async () => {
      const total = clamp(burstTotal, 1, 200);
      const concurrency = clamp(burstConcurrency, 1, 20);
      const run: BurstRun = {
        label: `${total} unique @ ${concurrency} concurrent`,
        total,
        concurrency,
        results: [],
        startedAt: Date.now(),
        finishedAt: null,
      };
      onBurst({ ...run });
      const prefix = newTransactionId("txn_burst");
      const tasks = Array.from({ length: total }, (_, i) => () => postWebhook(`burst ${i + 1}/${total}`, payload(`${prefix}_${i}`)));
      await runWithConcurrency(tasks, concurrency, (result) => {
        run.results.push(result);
        onResult(result);
        setProgress(`${run.results.length} / ${total} sent`);
        onBurst({ ...run, results: [...run.results] });
      });
      onBurst({ ...run, results: [...run.results], finishedAt: Date.now() });
    });

  const sendInvalid = () =>
    guarded(async () => {
      onResult(
        await timedRequest("invalid payload (expect 422)", "POST", "/v1/webhooks/transactions", {
          json: { transaction_id: "bad id with spaces", amount: -1, currency: "RUPEE" },
        }),
      );
    });

  const sendMalformed = () =>
    guarded(async () => {
      onResult(
        await timedRequest("malformed JSON (expect 422)", "POST", "/v1/webhooks/transactions", {
          rawBody: "{not json",
        }),
      );
    });

  const lookupUnknown = () =>
    guarded(async () => {
      const id = newTransactionId("txn_missing");
      onResult(await timedRequest("unknown id (expect 404)", "GET", `/v1/transactions/${id}`, { transactionId: id }));
    });

  const current = TABS.find((t) => t.id === tab)!;

  return (
    <section className="card action-panel">
      <div className="card-head">
        <h2>Send webhooks</h2>
        <span className="hint">replaces the curl commands</span>
      </div>
      <div className="tabs" role="tablist">
        {TABS.map((t) => (
          <button
            key={t.id}
            role="tab"
            aria-selected={tab === t.id}
            className={tab === t.id ? "tab active" : "tab"}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>
      <p className="criterion">{current.criterion}</p>

      {(tab === "single" || tab === "duplicates") && (
        <div className="form-grid">
          <label className="span-2">
            transaction_id
            <div className="with-button">
              <input value={txnId} onChange={(e) => setTxnId(e.target.value)} spellCheck={false} />
              <button type="button" className="ghost" title="Generate a new id" onClick={() => setTxnId(newTransactionId())}>
                ↻
              </button>
            </div>
          </label>
          <label>
            source_account
            <input value={source} onChange={(e) => setSource(e.target.value)} />
          </label>
          <label>
            destination_account
            <input value={destination} onChange={(e) => setDestination(e.target.value)} />
          </label>
          <label>
            amount
            <input type="number" min={0.01} step="0.01" value={amount} onChange={(e) => setAmount(Number(e.target.value))} />
          </label>
          <label>
            currency
            <input value={currency} maxLength={3} onChange={(e) => setCurrency(e.target.value.toUpperCase())} />
          </label>
        </div>
      )}

      {tab === "single" && (
        <button className="primary" disabled={busy} onClick={sendSingle}>
          POST webhook
        </button>
      )}

      {tab === "duplicates" && (
        <div className="row">
          <label className="inline">
            copies
            <input type="number" min={2} max={20} value={copies} onChange={(e) => setCopies(Number(e.target.value))} />
          </label>
          <button className="primary" disabled={busy} onClick={sendDuplicates}>
            POST {clamp(copies, 2, 20)}× same id
          </button>
        </div>
      )}

      {tab === "burst" && (
        <>
          <div className="row">
            <label className="inline">
              total
              <input type="number" min={1} max={200} value={burstTotal} onChange={(e) => setBurstTotal(Number(e.target.value))} />
            </label>
            <label className="inline">
              concurrency
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
          <p className="note">
            Unique ids, up to 200 requests and 20 in flight. Latency is measured in the browser, so it includes your
            network; the server-side time is shown in the header. Use k6 for a real load test.
          </p>
        </>
      )}

      {tab === "negative" && (
        <div className="button-stack">
          <button disabled={busy} onClick={sendInvalid}>
            Invalid payload → 422
          </button>
          <button disabled={busy} onClick={sendMalformed}>
            Malformed JSON → 422
          </button>
          <button disabled={busy} onClick={lookupUnknown}>
            Unknown transaction → 404
          </button>
        </div>
      )}

      {progress && <p className="progress-text">{progress}</p>}
    </section>
  );
}
