// React hooks: state, memoised filtering, DOM ref, and effects.
import { useEffect, useMemo, useRef, useState } from "react";
// Time formatter, aliased to the short name `clock` used in the render below.
import { clockTime as clock } from "../lib/time";
// Shape of one log line.
import type { LogEntry } from "../types";

// Values supplied by the parent.
interface Props {
  // All log entries collected so far.
  entries: LogEntry[];
  // Clears the log (state lives in the parent).
  onClear: () => void;
}

// Scrolling, filterable, pausable log of client requests and server events.
export function LiveLog({ entries, onClear }: Props) {
  // Whether the view is frozen.
  const [paused, setPaused] = useState(false);
  // Snapshot of entries taken at the moment of pausing.
  const [frozen, setFrozen] = useState<LogEntry[]>([]);
  // Free-text filter (e.g. a transaction id).
  const [filter, setFilter] = useState("");
  // Which log sources are currently visible.
  const [sources, setSources] = useState({ client: true, server: true, system: true });
  // Sentinel element at the end of the log used to auto-scroll.
  const bottom = useRef<HTMLDivElement>(null);

  // While paused show the frozen snapshot; otherwise the live list.
  const visibleSource = paused ? frozen : entries;
  // Recompute the filtered list only when its inputs change.
  const shown = useMemo(() => {
    // Normalise the search text for case-insensitive matching.
    const needle = filter.trim().toLowerCase();
    // Keep entries from enabled sources whose text matches (or all when no filter).
    return visibleSource.filter(
      (e) => sources[e.source] && (!needle || e.text.toLowerCase().includes(needle)),
    );
  }, [visibleSource, filter, sources]);

  // Auto-scroll to the newest line unless the user paused.
  useEffect(() => {
    // Optional chaining: ref may be null before mount.
    if (!paused) bottom.current?.scrollIntoView({ block: "end" });
  }, [shown, paused]);

  // Pause/resume handler.
  const togglePause = () => {
    // On pause, capture the current entries so the view stops moving.
    if (!paused) setFrozen(entries);
    // Flip the paused flag.
    setPaused(!paused);
  };

  return (
    <section className="card live-log">
      {/* Header: title plus filter controls */}
      <div className="card-head">
        <h2>Live log</h2>
        <div className="filters">
          {/* One checkbox per source; `as const` keeps keys typed for indexing */}
          {(["client", "server", "system"] as const).map((s) => (
            <label key={s} className={`check src-${s}`}>
              <input
                type="checkbox"
                // Reflect the current visibility of this source.
                checked={sources[s]}
                // Update only this source, preserving the others.
                onChange={(e) => setSources({ ...sources, [s]: e.target.checked })}
              />
              {s}
            </label>
          ))}
          {/* Text filter */}
          <input
            className="log-filter"
            placeholder="filter (e.g. a transaction id)"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          {/* Pause/resume; while paused shows how many entries arrived since */}
          <button className="chip" onClick={togglePause}>
            {paused ? `Resume (+${entries.length - frozen.length})` : "Pause"}
          </button>
          {/* Clear all entries */}
          <button className="chip" onClick={onClear}>
            Clear
          </button>
        </div>
      </div>
      {/* Scrollable log area */}
      <div className="log-body">
        {/* Empty state vs. list of lines */}
        {shown.length === 0 ? (
          <p className="empty">Waiting for activity… client requests and worker events stream in here.</p>
        ) : (
          shown.map((e) => (
            // Level class drives colour (info/warn/error)
            <div key={e.id} className={`log-line lvl-${e.level}`}>
              {/* Wall-clock time of the entry */}
              <span className="log-time">{clock(e.at)}</span>
              {/* Origin badge: client, server or system */}
              <span className={`log-src src-${e.source}`}>{e.source}</span>
              {/* Message body */}
              <span className="log-text">{e.text}</span>
            </div>
          ))
        )}
        {/* Scroll anchor for auto-scroll */}
        <div ref={bottom} />
      </div>
    </section>
  );
}
