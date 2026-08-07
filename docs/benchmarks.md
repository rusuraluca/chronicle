# Benchmark notes

Reproducible via:

```bash
# with Compose
docker compose -f deploy/docker-compose.yml up --build -d
make bench

# or against a local server
cargo run -p chronicle-server --release
COUNT=1000 ./scripts/bench.sh
```

`POST /v1/bench` ingests N events then starts a `max` speed replay over the same window, recording:

| Field | Meaning |
|-------|---------|
| `events_per_sec` | Durable ingest throughput for the run |
| `replay_latency_ms` | Wall time until max-speed replay completes |
| `storage_bytes` | Postgres relation size for `events` + `streams` |

## Sample local numbers

> Recorded 2026-08-07 on Apple Silicon against local Postgres 16 + Redis 7, `chronicle-server` **release** build, `COUNT=1000`, single-threaded HTTP client loop inside the server handler.

| Metric | Value |
|--------|-------|
| Ingest throughput | **934 events/sec** |
| Replay latency (`max`, 1000 events) | **265 ms** |
| Storage (`events` + `streams`) | **432 KiB** (442,368 bytes) |

Raw JSON from the run:

```json
{
  "label": "auto-1000",
  "events_per_sec": 934.1572132875923,
  "replay_latency_ms": 264.577542,
  "storage_bytes": 442368
}
```

These are **illustrative MVP numbers**, not a production ceiling. The current bottleneck is synchronous per-event HTTP handling plus a Redis `XADD` after each durable commit. Partitioning by `stream_id` and batching fan-out would raise the ceiling substantially.

Re-run before citing externally:

```bash
make bench
curl -s http://127.0.0.1:8080/v1/bench | jq .
```
