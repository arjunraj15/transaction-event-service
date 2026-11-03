// React hooks for state and lifecycle.
import { useEffect, useState } from "react";

/** Current time in ms, refreshed on an interval: drives countdowns and activity highlights. */
export function useNow(intervalMs = 250): number {
  // Lazy initializer so Date.now() is read once on mount, not on every render.
  const [now, setNow] = useState(() => Date.now());
  // Run the ticker as an effect so it starts on mount and restarts if the interval changes.
  useEffect(() => {
    // Update the clock every intervalMs, which re-renders consumers.
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    // Cleanup: stop the timer on unmount or interval change to avoid leaks.
    return () => clearInterval(timer);
  }, [intervalMs]);
  // Expose the latest timestamp to the caller.
  return now;
}
