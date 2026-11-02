import { useEffect, useState } from "react";
import { timedRequest } from "../api";
import type { RequestResult } from "../types";

/** Shows the brief's GET /v1/transactions/{id} response for one transaction, with a refresh button. */
export function InspectDrawer({ transactionId, onClose }: { transactionId: string; onClose: () => void }) {
  const [result, setResult] = useState<RequestResult | null>(null);

  const load = () =>
    void timedRequest("inspect", "GET", `/v1/transactions/${encodeURIComponent(transactionId)}`).then(setResult);

  useEffect(load, [transactionId]); // reload when a different row is inspected

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" onClick={(e) => e.stopPropagation()}>
        <div className="card-head">
          <h2>GET /v1/transactions/{"{id}"}</h2>
          <button className="ghost" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <p className="mono id">{transactionId}</p>
        {result ? (
          <>
            <p className="hint">
              HTTP {result.status} · {result.ms.toFixed(0)} ms ·{" "}
              <button className="link" onClick={load}>
                refresh
              </button>
            </p>
            <pre className="json">{JSON.stringify(result.body, null, 2)}</pre>
          </>
        ) : (
          <p className="hint">loading…</p>
        )}
      </aside>
    </div>
  );
}
