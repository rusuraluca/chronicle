# Benchmark notes

Reproducible via:

```bash
docker compose -f deploy/docker-compose.yml up --build -d
make bench
# or: COUNT=1000 ./scripts/bench.sh
```

`POST /v1/bench` ingests N events then starts a `max` speed replay over the same window, recording:

| Field | Meaning |
|-------|---------|
| `events_per_sec` | Durable ingest throughput for the run |
| `replay_latency_ms` | Wall time until max-speed replay completes |
| `storage_bytes` | Postgres relation size for `events` + `streams` |

## Sample numbers (Compose stack)

> Recorded 2026-08-08 on Apple Silicon via `docker compose` (Colima), `COUNT=500`.

| Metric | Value |
|--------|-------|
| Ingest throughput | **~1,185 events/sec** |
| Replay latency (`max`, 500 events) | **~55 ms** |
| Storage (`events` + `streams`) | **240 KiB** (245,760 bytes) |

```json
{
  "label": "auto-500",
  "events_per_sec": 1184.994176061723,
  "replay_latency_ms": 54.755609,
  "storage_bytes": 245760
}
```

## Earlier host-binary sample

> Same machine, `cargo run -p chronicle-server --release` against local Postgres/Redis, `COUNT=1000`.

| Metric | Value |
|--------|-------|
| Ingest throughput | **~934 events/sec** |
| Replay latency (`max`, 1000 events) | **~265 ms** |
| Storage | **432 KiB** (442,368 bytes) |

These are **illustrative MVP numbers**, not a production ceiling. The current bottleneck is synchronous per-event HTTP handling plus a Redis `XADD` after each durable commit.

Re-run before citing externally:

```bash
make bench
curl -s http://127.0.0.1:8080/v1/bench | jq .
```
