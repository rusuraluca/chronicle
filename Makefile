COMPOSE := docker compose -f deploy/docker-compose.yml
CARGO := cargo
export PATH := $(HOME)/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$(HOME)/.cargo/bin:$(PATH)

.PHONY: help up down logs build test fmt clippy bench demo cli-status seed

help:
	@echo "Chronicle targets:"
	@echo "  make up       - build and start the full stack"
	@echo "  make down     - stop the stack"
	@echo "  make logs     - follow server logs"
	@echo "  make test     - run Rust tests"
	@echo "  make fmt      - rustfmt"
	@echo "  make clippy   - clippy -D warnings"
	@echo "  make demo     - seed a stream and start a 10x replay"
	@echo "  make bench    - run ingest/replay benchmark via API"

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f server

build:
	$(CARGO) build --workspace

test:
	$(CARGO) test --workspace

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

seed:
	./scripts/seed_demo.sh

demo: seed
	./scripts/demo_replay.sh

bench:
	./scripts/bench.sh

cli-status:
	$(COMPOSE) exec server chronicle status --url http://127.0.0.1:8080
