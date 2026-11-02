import { useEffect, useMemo, useRef, useState } from "react";
import { clockTime as clock } from "../lib/time";
import type { LogEntry } from "../types";

interface Props {
  entries: LogEntry[];
  onClear: () => void;
}

export function LiveLog({ entries, onClear }: Props) {
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState<LogEntry[]>([]);
  const [filter, setFilter] = useState("");
  const [sources, setSources] = useState({ client: true, server: true, system: true });
  const bottom = useRef<HTMLDivElement>(null);

  const visibleSource = paused ? frozen : entries;
  const shown = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    return visibleSource.filter(
      (e) => sources[e.source] && (!needle || e.text.toLowerCase().includes(needle)),
    );
  }, [visibleSource, filter, sources]);

  useEffect(() => {
    if (!paused) bottom.current?.scrollIntoView({ block: "end" });
  }, [shown, paused]);

  const togglePause = () => {
    if (!paused) setFrozen(entries);
    setPaused(!paused);
  };

  return (
    <section className="card live-log">
      <div className="card-head">
        <h2>Live log</h2>
        <div className="filters">
          {(["client", "server", "system"] as const).map((s) => (
            <label key={s} className={`check src-${s}`}>
              <input
                type="checkbox"
                checked={sources[s]}
                onChange={(e) => setSources({ ...sources, [s]: e.target.checked })}
              />
              {s}
            </label>
          ))}
          <input
            className="log-filter"
            placeholder="filter (e.g. a transaction id)"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          <button className="chip" onClick={togglePause}>
            {paused ? `Resume (+${entries.length - frozen.length})` : "Pause"}
          </button>
          <button className="chip" onClick={onClear}>
            Clear
          </button>
        </div>
      </div>
      <div className="log-body">
        {shown.length === 0 ? (
          <p className="empty">Waiting for activity… client requests and worker events stream in here.</p>
        ) : (
          shown.map((e) => (
            <div key={e.id} className={`log-line lvl-${e.level}`}>
              <span className="log-time">{clock(e.at)}</span>
              <span className={`log-src src-${e.source}`}>{e.source}</span>
              <span className="log-text">{e.text}</span>
            </div>
          ))
        )}
        <div ref={bottom} />
      </div>
    </section>
  );
}
