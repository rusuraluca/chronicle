# Correctness contract

Chronicle treats correctness as a first-class product surface.

## Duplicate delivery

- Identity: `(stream_id, event_id)`
- First accept assigns `seq` and stores the payload
- Later delivers with the same id return the original `seq` with `duplicate: true`
- Alert type: `duplicate`

## Out-of-order arrival

- Watermark: stream `latest_event_time`
- If `event_time < watermark`, the event is still appended with `out_of_order: true`
- Watermark never moves backwards
- Alert type: `out_of_order`

## Replay

- Reads Postgres only, ordered by `(event_time ASC, seq ASC)`
- Preserves original `seq` / `event_id` (no renumbering)
- Pacing uses original inter-event gaps × speed factor (`1x` / `10x` / `100x`); `max` sleeps zero

## APIs

- Write-path flags on ingest responses
- `GET /v1/alerts` and stream stats counters
- Prometheus metrics: `chronicle_events_ingested_total`, `chronicle_duplicates_total`, `chronicle_out_of_order_total`, `chronicle_replay_events_total`
