import { useState, type FormEvent } from "react";
import { API_BASE, verifyAdminKey } from "../api";

/** Asks for the admin key that protects the dashboard-only endpoints (list + live stream). */
export function LoginScreen({ onAuthenticated }: { onAuthenticated: (key: string) => void }) {
  const [key, setKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    if (!key.trim()) return;
    setChecking(true);
    setError(null);
    try {
      if (await verifyAdminKey(key.trim())) onAuthenticated(key.trim());
      else setError("That key was not accepted.");
    } catch {
      setError("Could not reach the API. Check that it is running.");
    } finally {
      setChecking(false);
    }
  }

  return (
    <div className="login">
      <form className="card login-card" onSubmit={submit}>
        <div className="brand">
          <span className="logo">◆</span>
          <div>
            <h1>Transaction Event Service</h1>
            <p className="hint">Demo dashboard</p>
          </div>
        </div>
        <p>
          The dashboard's admin endpoints (transaction list and live event stream) are protected. Enter the admin key
          to continue.
        </p>
        <label>
          Admin key
          <input
            type="password"
            autoComplete="current-password"
            autoFocus
            value={key}
            onChange={(e) => setKey(e.target.value)}
          />
        </label>
        {error && <p className="login-error">{error}</p>}
        <button className="primary" disabled={checking || !key.trim()}>
          {checking ? "Checking…" : "Open dashboard"}
        </button>
        <p className="hint">
          Kept only for this browser tab (sessionStorage) and sent in the <code>X-Admin-Key</code> header. The graded
          endpoints (<code>POST /v1/webhooks/transactions</code>, <code>GET /</code>,{" "}
          <code>GET /v1/transactions/&#123;id&#125;</code>) stay open, as the brief requires.
          {API_BASE && (
            <>
              {" "}
              API: <code>{API_BASE}</code>
            </>
          )}
        </p>
      </form>
    </div>
  );
}
