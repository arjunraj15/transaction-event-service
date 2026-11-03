// Hooks for local state and effects.
import { useEffect, useState } from "react";
// Generic timed HTTP helper from the API layer.
import { timedRequest } from "../api";
// Type of a recorded request/response.
import type { RequestResult } from "../types";

/** Shows the brief's GET /v1/transactions/{id} response for one transaction, with a refresh button. */
export function InspectDrawer({ transactionId, onClose }: { transactionId: string; onClose: () => void }) {
  // Latest lookup result; null while loading.
  const [result, setResult] = useState<RequestResult | null>(null);

  // Fetch the transaction and store the response (void: fire and forget).
  const load = () =>
    // encodeURIComponent keeps odd ids from breaking the URL path.
    void timedRequest("inspect", "GET", `/v1/transactions/${encodeURIComponent(transactionId)}`).then(setResult);

  useEffect(load, [transactionId]); // reload when a different row is inspected

  // Close the drawer when Escape is pressed.
  useEffect(() => {
    // Handler: short-circuits so onClose only runs for Escape.
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    // Listen globally since focus may be anywhere.
    window.addEventListener("keydown", onKey);
    // Cleanup on unmount or when onClose changes.
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    // Clicking the dimmed backdrop closes the drawer
    <div className="drawer-backdrop" onClick={onClose}>
      {/* stopPropagation so clicks inside the drawer don't reach the backdrop */}
      <aside className="drawer" onClick={(e) => e.stopPropagation()}>
        {/* Header with endpoint title and close button */}
        <div className="card-head">
          {/* {"{id}"} renders a literal {id} placeholder */}
          <h2>GET /v1/transactions/{"{id}"}</h2>
          <button className="ghost" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        {/* The transaction being inspected */}
        <p className="mono id">{transactionId}</p>
        {/* Show the response once loaded, otherwise a loading message */}
        {result ? (
          <>
            {/* Status, latency and a manual refresh link */}
            <p className="hint">
              HTTP {result.status} · {result.ms.toFixed(0)} ms ·{" "}
              <button className="link" onClick={load}>
                refresh
              </button>
            </p>
            {/* Pretty-printed JSON response body */}
            <pre className="json">{JSON.stringify(result.body, null, 2)}</pre>
          </>
        ) : (
          <p className="hint">loading…</p>
        )}
      </aside>
    </div>
  );
}
