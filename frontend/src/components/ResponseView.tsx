// Type of a recorded request/response.
import type { RequestResult } from "../types";

// Map HTTP status to a CSS class: 2xx ok, 4xx warn, everything else (5xx, network error) error.
const statusClass = (s: number) => (s >= 200 && s < 300 ? "ok" : s >= 400 && s < 500 ? "warn" : "error");

/** The most recent responses, with the raw body exactly as the API returned it. */
export function ResponseView({ results }: { results: RequestResult[] }) {
  // Newest result (undefined when the list is empty).
  const latest = results[results.length - 1];
  return (
    <section className="card response-view">
      {/* Header with session count */}
      <div className="card-head">
        <h2>Last responses</h2>
        <span className="hint">{results.length ? `${results.length} this session` : "nothing sent yet"}</span>
      </div>
      {/* Show data when available, otherwise a placeholder */}
      {latest ? (
        <>
          {/* Compact list of the last six responses, newest first */}
          <ul className="response-list">
            {results
              .slice(-6)
              .reverse()
              .map((r) => (
                <li key={r.id}>
                  {/* Status code; 0 (no response) shows as ERR */}
                  <span className={`status ${statusClass(r.status)}`}>{r.status || "ERR"}</span>
                  {/* HTTP method */}
                  <span className="method">{r.method}</span>
                  {/* Description of the scenario */}
                  <span className="label">{r.label}</span>
                  {/* Round-trip latency */}
                  <span className="ms">{r.ms.toFixed(0)} ms</span>
                </li>
              ))}
          </ul>
          {/* Full body of the most recent response, pretty-printed */}
          <pre className="json">{JSON.stringify(latest.body, null, 2)}</pre>
        </>
      ) : (
        <p className="empty">Responses appear here: status, latency and the raw JSON body.</p>
      )}
    </section>
  );
}
