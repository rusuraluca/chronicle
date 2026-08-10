-- Chronicle append-only event log (Postgres is source of truth).

CREATE TABLE IF NOT EXISTS streams (
    stream_id       TEXT PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latest_seq      BIGINT NOT NULL DEFAULT 0,
    latest_event_time TIMESTAMPTZ,
    event_count     BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    out_of_order_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    stream_id       TEXT NOT NULL REFERENCES streams(stream_id),
    seq             BIGINT NOT NULL,
    event_id        TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingest_time     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload         JSONB NOT NULL,
    out_of_order    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (stream_id, seq),
    UNIQUE (stream_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_stream_event_time
    ON events (stream_id, event_time ASC, seq ASC);

CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    stream_id       TEXT NOT NULL,
    alert_type      TEXT NOT NULL,
    event_id        TEXT,
    seq             BIGINT,
    message         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);

CREATE TABLE IF NOT EXISTS replays (
    replay_id       TEXT PRIMARY KEY,
    stream_id       TEXT NOT NULL,
    from_time       TIMESTAMPTZ NOT NULL,
    to_time         TIMESTAMPTZ NOT NULL,
    speed           TEXT NOT NULL,
    status          TEXT NOT NULL,
    events_emitted  BIGINT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bench_runs (
    id              BIGSERIAL PRIMARY KEY,
    label           TEXT NOT NULL,
    events_per_sec  DOUBLE PRECISION NOT NULL,
    replay_latency_ms DOUBLE PRECISION NOT NULL,
    storage_bytes   BIGINT NOT NULL,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
