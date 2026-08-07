# Chronicle

**Chronicle** is an ordered, append-only streaming event log with correctness checks and variable-speed replay.

Repository: https://github.com/rusuraluca/chronicle

## Quick start (Docker-first)

```bash
git clone https://github.com/rusuraluca/chronicle.git
cd chronicle
docker compose -f deploy/docker-compose.yml up --build
```

| Service   | URL                   |
|-----------|-----------------------|
| HTTP API  | http://localhost:8080 |
| Dashboard | http://localhost:3000 |

```bash
curl -s http://localhost:8080/healthz
make up && make test && make down
```

## Status

This branch is the **scaffold**: workspace, Docker Compose, CI, health endpoints, and placeholder UI.
Feature PRs stack on top: ingest → gRPC/Redis → replay → correctness surface → CLI → dashboard → docs/bench.

## Workspace

| Path | Role |
|------|------|
| `crates/chronicle-core` | Shared types |
| `crates/chronicle-server` | HTTP API |
| `crates/chronicle-cli` | Operator CLI |
| `dashboard/` | Ops UI |
| `deploy/` | Compose + images |

## License

Apache-2.0 — see [LICENSE](LICENSE).
