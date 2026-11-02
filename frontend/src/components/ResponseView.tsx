import type { RequestResult } from "../types";

const statusClass = (s: number) => (s >= 200 && s < 300 ? "ok" : s >= 400 && s < 500 ? "warn" : "error");

/** The most recent responses, with the raw body exactly as the API returned it. */
export function ResponseView({ results }: { results: RequestResult[] }) {
  const latest = results[results.length - 1];
  return (
    <section className="card response-view">
      <div className="card-head">
        <h2>Last responses</h2>
        <span className="hint">{results.length ? `${results.length} this session` : "nothing sent yet"}</span>
      </div>
      {latest ? (
        <>
          <ul className="response-list">
            {results
              .slice(-6)
              .reverse()
              .map((r) => (
                <li key={r.id}>
                  <span className={`status ${statusClass(r.status)}`}>{r.status || "ERR"}</span>
                  <span className="method">{r.method}</span>
                  <span className="label">{r.label}</span>
                  <span className="ms">{r.ms.toFixed(0)} ms</span>
                </li>
              ))}
          </ul>
          <pre className="json">{JSON.stringify(latest.body, null, 2)}</pre>
        </>
      ) : (
        <p className="empty">Responses appear here: status, latency and the raw JSON body.</p>
      )}
    </section>
  );
}
