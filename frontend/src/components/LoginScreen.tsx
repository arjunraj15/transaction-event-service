// useState plus the FormEvent type (inline `type` import keeps it type-only).
import { useState, type FormEvent } from "react";
// API base URL (for display) and the admin key check.
import { API_BASE, verifyAdminKey } from "../api";

/** Asks for the admin key that protects the dashboard-only endpoints (list + live stream). */
export function LoginScreen({ onAuthenticated }: { onAuthenticated: (key: string) => void }) {
  // Text typed into the key field.
  const [key, setKey] = useState("");
  // Error message to display, if any.
  const [error, setError] = useState<string | null>(null);
  // True while the key is being verified.
  const [checking, setChecking] = useState(false);

  // Form submit handler.
  async function submit(e: FormEvent) {
    // Stop the browser's default page-reloading submit.
    e.preventDefault();
    // Ignore empty or whitespace-only keys.
    if (!key.trim()) return;
    // Show the checking state and clear any previous error.
    setChecking(true);
    setError(null);
    try {
      // Accepted: hand the key to the parent; rejected: show a message.
      if (await verifyAdminKey(key.trim())) onAuthenticated(key.trim());
      else setError("That key was not accepted.");
    } catch {
      // Network/other failure, distinct from a wrong key.
      setError("Could not reach the API. Check that it is running.");
    } finally {
      // Always re-enable the form.
      setChecking(false);
    }
  }

  return (
    <div className="login">
      {/* Card form; Enter key triggers submit */}
      <form className="card login-card" onSubmit={submit}>
        {/* Branding */}
        <div className="brand">
          <span className="logo">◆</span>
          <div>
            <h1>Transaction Event Service</h1>
            <p className="hint">Demo dashboard</p>
          </div>
        </div>
        {/* Explanation of why a key is needed */}
        <p>
          The dashboard's admin endpoints (transaction list and live event stream) are protected. Enter the admin key
          to continue.
        </p>
        <label>
          Admin key
          <input
            // Mask the key as it is typed.
            type="password"
            // Hint for password managers.
            autoComplete="current-password"
            // Focus the field on load.
            autoFocus
            value={key}
            onChange={(e) => setKey(e.target.value)}
          />
        </label>
        {/* Error message, only when set */}
        {error && <p className="login-error">{error}</p>}
        {/* Submit button (default type); disabled while checking or empty */}
        <button className="primary" disabled={checking || !key.trim()}>
          {checking ? "Checking…" : "Open dashboard"}
        </button>
        {/* Footnote on key storage and which endpoints stay open */}
        <p className="hint">
          Kept only for this browser tab (sessionStorage) and sent in the <code>X-Admin-Key</code> header. The graded
          endpoints (<code>POST /v1/webhooks/transactions</code>, <code>GET /</code>,{" "}
          <code>GET /v1/transactions/&#123;id&#125;</code>) stay open, as the brief requires.
          {/* Show the API base URL only when one is configured */}
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
