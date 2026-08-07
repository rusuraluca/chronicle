COMPOSE := docker compose -f deploy/docker-compose.yml
CARGO := cargo
export PATH := $(HOME)/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$(HOME)/.cargo/bin:$(PATH)

.PHONY: help up down logs build test fmt clippy

help:
	@echo "Chronicle targets:"
	@echo "  make up       - build and start the full stack"
	@echo "  make down     - stop the stack"
	@echo "  make logs     - follow server logs"
	@echo "  make test     - run Rust tests"
	@echo "  make fmt      - rustfmt"
	@echo "  make clippy   - clippy -D warnings"

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
