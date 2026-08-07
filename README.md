# Chronicle

**Chronicle** is an open-source ordered event log with duplicate / out-of-order detection and variable-speed replay.

> First impression this repo should leave: *she understands production data pipelines.*

Repository: https://github.com/rusuraluca/chronicle

## What it does

```mermaid
flowchart LR
  Producer["Producer / CLI"] --> Ingest["Ingest API REST + gRPC"]
  Ingest --> PgLog["Postgres append-only log"]
  Ingest --> RedisHot["Redis Streams"]
  PgLog --> Replay["Replay engine 1x/10x/100x/max"]
  RedisHot --> Consumers["Consumers"]
  Replay --> Consumers
  Ingest --> Correctness["Dedup + OOO alerts"]
  Correctness --> Dash["Dashboard"]
  Replay --> Dash
```

- Append-only log with monotonic per-stream `seq`
- REST + gRPC ingest
- Replay from `T1`→`T2` at `1x` / `10x` / `100x` / `max`
- Idempotent duplicate detection + out-of-order alerting
- CLI: `chronicle ingest|replay|status`
- Dashboard: lag, throughput, replay status, benchmark page

Deep dive: [docs/architecture.md](docs/architecture.md) · Numbers: [docs/benchmarks.md](docs/benchmarks.md)

## Quick start (Docker-first)

```bash
git clone https://github.com/rusuraluca/chronicle.git
cd chronicle
cp .env.example .env
docker compose -f deploy/docker-compose.yml up --build
```

| Service   | URL                   |
|-----------|-----------------------|
| HTTP API  | http://localhost:8080 |
| gRPC      | localhost:50051       |
| Dashboard | http://localhost:3000 |
| Metrics   | http://localhost:8080/metrics |

```bash
# health
curl -s http://localhost:8080/healthz

# ingest
curl -s -X POST http://localhost:8080/v1/streams/orders/events \
  -H 'content-type: application/json' \
  -d '{"event_id":"e1","event_time":"2026-08-07T12:00:00Z","payload":{"sku":"A"}}'

# CLI (inside the server container)
docker compose -f deploy/docker-compose.yml exec server \
  chronicle ingest --url http://127.0.0.1:8080 --stream orders --payload '{"sku":"B"}'

# seed + 10x replay demo
make demo
```

Makefile shortcuts: `make up`, `make test`, `make bench`, `make down`.

## Local development (without full Compose app)

Requires Rust stable, Postgres, Redis:

```bash
export DATABASE_URL=postgres://chronicle:chronicle@127.0.0.1:5432/chronicle
export REDIS_URL=redis://127.0.0.1:6379
cargo run -p chronicle-server
cargo run -p chronicle-cli -- status
cd dashboard && npm install && npm run dev
```

## Tests

```bash
export DATABASE_URL=postgres://chronicle:chronicle@127.0.0.1:5432/chronicle
export REDIS_URL=redis://127.0.0.1:6379
# optional: fail instead of skip if services are down
export CHRONICLE_REQUIRE_INTEGRATION=1
make test
```

Coverage includes:

- **Unit:** correctness (duplicate / OOO / watermark), replay pacing + speed parsing
- **Integration:** ordered `seq`, idempotent duplicate ingest, OOO alerts, REST round-trip, max-speed replay completion, `/healthz`

CI (GitHub Actions) runs `fmt`, `clippy -D warnings`, `cargo test` with Postgres/Redis service containers, dashboard build + smoke, and Docker image builds. On `main`, CD pushes server/dashboard images to GHCR.

## Workspace

| Path | Role |
|------|------|
| `crates/chronicle-core` | Shared types, correctness, pacing |
| `crates/chronicle-server` | REST, gRPC, Postgres log, Redis fan-out, replay |
| `crates/chronicle-cli` | Operator CLI |
| `dashboard/` | Ops + benchmark UI |
| `proto/` | gRPC contract |
| `migrations/` | SQL schema |
| `deploy/` | Docker Compose + images |

## Correctness contract (short)

| Signal | Behavior |
|--------|----------|
| Duplicate `event_id` | Idempotent ack; alert + metric; no new `seq` |
| Out-of-order `event_time` | Accepted, flagged, alerted; watermark does not move backwards |
| Replay | Reads Postgres only; preserves `seq` / `event_id`; paced by original gaps |

## License

Apache-2.0 — see [LICENSE](LICENSE).
