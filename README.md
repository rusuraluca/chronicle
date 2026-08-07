# Chronicle

**Chronicle** is an ordered, append-only streaming event log with correctness checks and variable-speed replay.

Repository: https://github.com/rusuraluca/chronicle

## Quick start (Docker-first)

```bash
docker compose -f deploy/docker-compose.yml up --build
```

| Service   | URL                         |
|-----------|-----------------------------|
| HTTP API  | http://localhost:8080       |
| gRPC      | localhost:50051             |
| Dashboard | http://localhost:3000       |

Health: `curl -s http://localhost:8080/healthz`

```bash
make up
make test
make down
```

## Tests

```bash
# Unit + integration (Postgres + Redis required for integration)
export DATABASE_URL=postgres://chronicle:chronicle@127.0.0.1:5432/chronicle
export REDIS_URL=redis://127.0.0.1:6379
make test   # or: cargo test --workspace
```

CI runs `fmt`, `clippy -D warnings`, `cargo test` (with Postgres/Redis services), dashboard build, and Docker image smoke builds.

## Workspace

- `crates/chronicle-core` — shared types, correctness, pacing
- `crates/chronicle-server` — REST, gRPC, Postgres log, Redis fan-out, replay
- `crates/chronicle-cli` — `chronicle ingest|replay|status`
- `dashboard/` — ops + benchmark UI

## License

Apache-2.0 — see [LICENSE](LICENSE).
