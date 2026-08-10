-- Scaffold schema: stream registry only. Event log arrives in the ingest PR.

CREATE TABLE IF NOT EXISTS streams (
    stream_id         TEXT PRIMARY KEY,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latest_seq        BIGINT NOT NULL DEFAULT 0,
    latest_event_time TIMESTAMPTZ,
    event_count       BIGINT NOT NULL DEFAULT 0,
    duplicate_count   BIGINT NOT NULL DEFAULT 0,
    out_of_order_count BIGINT NOT NULL DEFAULT 0
);
