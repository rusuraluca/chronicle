import { useEffect, useState, useTransition } from "react";
import {
  type BenchRun,
  type SystemStatus,
  fetchBench,
  fetchStatus,
  runBench,
} from "./api";

type View = "ops" | "bench";

export default function App() {
  const [view, setView] = useState<View>("ops");
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [bench, setBench] = useState<BenchRun | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const next = await fetchStatus();
        if (!cancelled) {
          setStatus(next);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
        }
      }
    };
    void tick();
    const id = window.setInterval(() => void tick(), 2000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    void fetchBench()
      .then(setBench)
      .catch(() => setBench(null));
  }, []);

  const totalEvents = status?.streams.reduce((n, s) => n + s.event_count, 0) ?? 0;
  const totalDupes = status?.streams.reduce((n, s) => n + s.duplicate_count, 0) ?? 0;
  const totalOoo = status?.streams.reduce((n, s) => n + s.out_of_order_count, 0) ?? 0;
  const redisTotal = status?.redis_lengths.reduce((n, s) => n + s.xlen, 0) ?? 0;

  return (
    <div className="shell">
      <header className="brand-bar">
        <div>
          <p className="brand">Chronicle</p>
          <p className="tagline">Ordered event log · correctness · variable-speed replay</p>
        </div>
        <nav className="nav">
          <button
            type="button"
            className={view === "ops" ? "active" : ""}
            onClick={() => setView("ops")}
          >
            Operations
          </button>
          <button
            type="button"
            className={view === "bench" ? "active" : ""}
            onClick={() => setView("bench")}
          >
            Benchmark
          </button>
        </nav>
      </header>

      {error && <p className="error">API unreachable: {error}</p>}

      {view === "ops" && (
        <main className="panel">
          <section className="hero-metrics" aria-label="Live pipeline metrics">
            <Metric label="Events stored" value={totalEvents.toLocaleString()} />
            <Metric label="Redis hot length" value={redisTotal.toLocaleString()} />
            <Metric label="Duplicates" value={totalDupes.toLocaleString()} />
            <Metric label="Out of order" value={totalOoo.toLocaleString()} />
            <Metric
              label="Uptime"
              value={status ? `${status.uptime_secs}s` : "—"}
            />
            <Metric
              label="Storage"
              value={status ? formatBytes(status.storage_bytes) : "—"}
            />
          </section>

          <section>
            <h2>Streams</h2>
            <p className="section-note">Per-stream lag signals from the durable log and Redis fan-out.</p>
            <table>
              <thead>
                <tr>
                  <th>Stream</th>
                  <th>Events</th>
                  <th>Latest seq</th>
                  <th>Dupes</th>
                  <th>OOO</th>
                  <th>Redis xlen</th>
                </tr>
              </thead>
              <tbody>
                {(status?.streams ?? []).map((s) => {
                  const xlen =
                    status?.redis_lengths.find((r) => r.stream_id === s.stream_id)?.xlen ?? 0;
                  return (
                    <tr key={s.stream_id}>
                      <td>{s.stream_id}</td>
                      <td>{s.event_count}</td>
                      <td>{s.latest_seq ?? "—"}</td>
                      <td>{s.duplicate_count}</td>
                      <td>{s.out_of_order_count}</td>
                      <td>{xlen}</td>
                    </tr>
                  );
                })}
                {(status?.streams.length ?? 0) === 0 && (
                  <tr>
                    <td colSpan={6}>No streams yet — ingest with the CLI or REST API.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>

          <section>
            <h2>Replay status</h2>
            <p className="section-note">Active and recent replays reading from Postgres in deterministic order.</p>
            <ul className="list">
              {(status?.active_replays.length
                ? status.active_replays
                : []
              ).map((r) => (
                <li key={r.replay_id}>
                  <strong>{r.stream_id}</strong> · {r.status} · {r.speed} · emitted{" "}
                  {r.events_emitted}
                </li>
              ))}
              {(status?.active_replays.length ?? 0) === 0 && (
                <li>No active replays.</li>
              )}
            </ul>
          </section>

          <section>
            <h2>Correctness alerts</h2>
            <p className="section-note">Duplicate delivery and out-of-order event_time regressions.</p>
            <ul className="list alerts">
              {(status?.recent_alerts ?? []).map((a) => (
                <li key={a.id}>
                  <span className={`pill ${a.alert_type}`}>{a.alert_type}</span>
                  {a.message}
                </li>
              ))}
              {(status?.recent_alerts.length ?? 0) === 0 && <li>No alerts.</li>}
            </ul>
          </section>
        </main>
      )}

      {view === "bench" && (
        <main className="panel">
          <section>
            <h2>Benchmark</h2>
            <p className="section-note">
              Measure ingest throughput, max-speed replay latency, and estimated storage cost.
            </p>
            <button
              type="button"
              className="primary"
              disabled={pending}
              onClick={() => {
                startTransition(async () => {
                  try {
                    const result = await runBench(1000);
                    setBench(result);
                    setError(null);
                  } catch (err) {
                    setError(err instanceof Error ? err.message : String(err));
                  }
                });
              }}
            >
              {pending ? "Running…" : "Run 1k-event bench"}
            </button>
            {bench && (
              <div className="hero-metrics bench-grid">
                <Metric
                  label="Events / sec"
                  value={bench.events_per_sec.toFixed(0)}
                />
                <Metric
                  label="Replay latency"
                  value={`${bench.replay_latency_ms.toFixed(1)} ms`}
                />
                <Metric label="Storage" value={formatBytes(bench.storage_bytes)} />
                <Metric label="Label" value={bench.label} />
              </div>
            )}
          </section>
        </main>
      )}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span className="metric-label">{label}</span>
      <span className="metric-value">{value}</span>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}
