const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${path} failed: ${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

export type StreamStats = {
  stream_id: string;
  event_count: number;
  latest_seq: number | null;
  latest_event_time: string | null;
  duplicate_count: number;
  out_of_order_count: number;
};

export type ReplayStatus = {
  replay_id: string;
  stream_id: string;
  from: string;
  to: string;
  speed: string;
  status: string;
  events_emitted: number;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
};

export type Alert = {
  id: number;
  stream_id: string;
  alert_type: string;
  event_id: string | null;
  seq: number | null;
  message: string;
  created_at: string;
};

export type BenchRun = {
  id: number;
  label: string;
  events_per_sec: number;
  replay_latency_ms: number;
  storage_bytes: number;
  notes: string | null;
  created_at: string;
};

export type SystemStatus = {
  uptime_secs: number;
  streams: StreamStats[];
  active_replays: ReplayStatus[];
  recent_alerts: Alert[];
  storage_bytes: number;
  redis_lengths: { stream_id: string; xlen: number }[];
};

export function fetchStatus() {
  return getJson<SystemStatus>("/v1/status");
}

export function fetchBench() {
  return getJson<BenchRun | null>("/v1/bench");
}

export function runBench(count = 1000) {
  return postJson<BenchRun>("/v1/bench", { count, stream_id: "bench" });
}
