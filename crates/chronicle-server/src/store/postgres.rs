use chronicle_core::{
    evaluate_event, CorrectnessDecision, EventEnvelope, EventRecord, IngestResponse, ReplayRequest,
    ReplaySpeed, ReplayStatus, ReplayStatusKind, StreamStats, StreamWatermark,
};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
use uuid::Uuid;

#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
}

impl EventStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        // Serialize schema bootstrap across parallel integration tests / pods.
        sqlx::query("SELECT pg_advisory_lock(872364102)")
            .execute(&self.pool)
            .await?;
        let result = async {
            let sql = include_str!("../../../../migrations/001_init.sql");
            sqlx::raw_sql(sql).execute(&self.pool).await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        sqlx::query("SELECT pg_advisory_unlock(872364102)")
            .execute(&self.pool)
            .await?;
        result
    }

    pub async fn ensure_stream(&self, stream_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO streams (stream_id)
            VALUES ($1)
            ON CONFLICT (stream_id) DO NOTHING
            "#,
        )
        .bind(stream_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn watermark(&self, stream_id: &str) -> anyhow::Result<StreamWatermark> {
        let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<i64>)>(
            r#"
            SELECT latest_event_time, CASE WHEN latest_seq = 0 THEN NULL ELSE latest_seq END
            FROM streams
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some((last_event_time, last_seq)) => StreamWatermark {
                last_event_time,
                last_seq,
            },
            None => StreamWatermark::default(),
        })
    }

    pub async fn find_by_event_id(
        &self,
        stream_id: &str,
        event_id: &str,
    ) -> anyhow::Result<Option<EventRecord>> {
        let row = sqlx::query_as::<_, EventRow>(
            r#"
            SELECT stream_id, seq, event_id, event_time, ingest_time, payload, out_of_order
            FROM events
            WHERE stream_id = $1 AND event_id = $2
            "#,
        )
        .bind(stream_id)
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    /// Durable append with per-stream sequence assignment and correctness checks.
    pub async fn append(
        &self,
        stream_id: &str,
        envelope: EventEnvelope,
    ) -> anyhow::Result<IngestResponse> {
        let mut tx = self.pool.begin().await?;
        self.ensure_stream_tx(&mut tx, stream_id).await?;

        if let Some(existing) = self
            .find_by_event_id_tx(&mut tx, stream_id, &envelope.event_id)
            .await?
        {
            let _ = evaluate_event(&envelope, &StreamWatermark::default(), true);
            sqlx::query(
                r#"
                UPDATE streams
                SET duplicate_count = duplicate_count + 1
                WHERE stream_id = $1
                "#,
            )
            .bind(stream_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO alerts (stream_id, alert_type, event_id, seq, message)
                VALUES ($1, 'duplicate', $2, $3, $4)
                "#,
            )
            .bind(stream_id)
            .bind(&envelope.event_id)
            .bind(existing.seq)
            .bind(format!(
                "Duplicate event_id `{}` (existing seq {})",
                envelope.event_id, existing.seq
            ))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            metrics::counter!("chronicle_duplicates_total", "stream" => stream_id.to_string())
                .increment(1);

            return Ok(IngestResponse {
                stream_id: stream_id.to_string(),
                seq: existing.seq,
                event_id: existing.event_id,
                duplicate: true,
                out_of_order: false,
            });
        }

        let watermark = self.watermark_tx(&mut tx, stream_id).await?;
        let outcome = evaluate_event(&envelope, &watermark, false);
        let next_seq = watermark.last_seq.unwrap_or(0) + 1;

        let ingest_time = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO events (stream_id, seq, event_id, event_time, ingest_time, payload, out_of_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(stream_id)
        .bind(next_seq)
        .bind(&envelope.event_id)
        .bind(envelope.event_time)
        .bind(ingest_time)
        .bind(&envelope.payload)
        .bind(outcome.out_of_order)
        .execute(&mut *tx)
        .await?;

        let new_latest_event_time = match watermark.last_event_time {
            Some(prev) if envelope.event_time < prev => prev,
            _ => envelope.event_time,
        };

        sqlx::query(
            r#"
            UPDATE streams
            SET latest_seq = $2,
                latest_event_time = $3,
                event_count = event_count + 1,
                out_of_order_count = out_of_order_count + CASE WHEN $4 THEN 1 ELSE 0 END
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .bind(next_seq)
        .bind(new_latest_event_time)
        .bind(outcome.out_of_order)
        .execute(&mut *tx)
        .await?;

        if outcome.decision == CorrectnessDecision::AcceptOutOfOrder {
            sqlx::query(
                r#"
                INSERT INTO alerts (stream_id, alert_type, event_id, seq, message)
                VALUES ($1, 'out_of_order', $2, $3, $4)
                "#,
            )
            .bind(stream_id)
            .bind(&envelope.event_id)
            .bind(next_seq)
            .bind(format!(
                "Out-of-order event_time {} (watermark {:?})",
                envelope.event_time, watermark.last_event_time
            ))
            .execute(&mut *tx)
            .await?;
            metrics::counter!("chronicle_out_of_order_total", "stream" => stream_id.to_string())
                .increment(1);
        }

        tx.commit().await?;
        metrics::counter!("chronicle_events_ingested_total", "stream" => stream_id.to_string())
            .increment(1);

        Ok(IngestResponse {
            stream_id: stream_id.to_string(),
            seq: next_seq,
            event_id: envelope.event_id,
            duplicate: false,
            out_of_order: outcome.out_of_order,
        })
    }

    pub async fn events_in_range(
        &self,
        stream_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> anyhow::Result<Vec<EventRecord>> {
        let rows = sqlx::query_as::<_, EventRow>(
            r#"
            SELECT stream_id, seq, event_id, event_time, ingest_time, payload, out_of_order
            FROM events
            WHERE stream_id = $1
              AND event_time >= $2
              AND event_time <= $3
            ORDER BY event_time ASC, seq ASC
            "#,
        )
        .bind(stream_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn stream_stats(&self, stream_id: &str) -> anyhow::Result<Option<StreamStats>> {
        let row = sqlx::query_as::<_, StreamStatsRow>(
            r#"
            SELECT stream_id, event_count, latest_seq, latest_event_time,
                   duplicate_count, out_of_order_count
            FROM streams
            WHERE stream_id = $1
            "#,
        )
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_stream_stats(&self) -> anyhow::Result<Vec<StreamStats>> {
        let rows = sqlx::query_as::<_, StreamStatsRow>(
            r#"
            SELECT stream_id, event_count, latest_seq, latest_event_time,
                   duplicate_count, out_of_order_count
            FROM streams
            ORDER BY stream_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn create_replay(&self, req: &ReplayRequest) -> anyhow::Result<ReplayStatus> {
        let replay_id = Uuid::new_v4().to_string();
        let speed = speed_to_str(req.speed);
        sqlx::query(
            r#"
            INSERT INTO replays (replay_id, stream_id, from_time, to_time, speed, status)
            VALUES ($1, $2, $3, $4, $5, 'pending')
            "#,
        )
        .bind(&replay_id)
        .bind(&req.stream_id)
        .bind(req.from)
        .bind(req.to)
        .bind(speed)
        .execute(&self.pool)
        .await?;

        Ok(ReplayStatus {
            replay_id,
            stream_id: req.stream_id.clone(),
            from: req.from,
            to: req.to,
            speed: req.speed,
            status: ReplayStatusKind::Pending,
            events_emitted: 0,
            started_at: None,
            finished_at: None,
            error: None,
        })
    }

    pub async fn update_replay(&self, status: &ReplayStatus) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE replays
            SET status = $2,
                events_emitted = $3,
                started_at = $4,
                finished_at = $5,
                error = $6
            WHERE replay_id = $1
            "#,
        )
        .bind(&status.replay_id)
        .bind(status_to_str(status.status))
        .bind(status.events_emitted)
        .bind(status.started_at)
        .bind(status.finished_at)
        .bind(&status.error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_replay(&self, replay_id: &str) -> anyhow::Result<Option<ReplayStatus>> {
        let row = sqlx::query_as::<_, ReplayRow>(
            r#"
            SELECT replay_id, stream_id, from_time, to_time, speed, status,
                   events_emitted, started_at, finished_at, error
            FROM replays
            WHERE replay_id = $1
            "#,
        )
        .bind(replay_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| r.try_into()).transpose()
    }

    pub async fn list_replays(&self, limit: i64) -> anyhow::Result<Vec<ReplayStatus>> {
        let rows = sqlx::query_as::<_, ReplayRow>(
            r#"
            SELECT replay_id, stream_id, from_time, to_time, speed, status,
                   events_emitted, started_at, finished_at, error
            FROM replays
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|r| r.try_into()).collect()
    }

    pub async fn recent_alerts(&self, limit: i64) -> anyhow::Result<Vec<Alert>> {
        let rows = sqlx::query_as::<_, Alert>(
            r#"
            SELECT id, stream_id, alert_type, event_id, seq, message, created_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn storage_bytes(&self) -> anyhow::Result<i64> {
        let (bytes,): (i64,) = sqlx::query_as(
            r#"
            SELECT COALESCE(pg_total_relation_size('events'), 0)
                 + COALESCE(pg_total_relation_size('streams'), 0)
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(bytes)
    }

    pub async fn record_bench(
        &self,
        label: &str,
        events_per_sec: f64,
        replay_latency_ms: f64,
        storage_bytes: i64,
        notes: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bench_runs (label, events_per_sec, replay_latency_ms, storage_bytes, notes)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(label)
        .bind(events_per_sec)
        .bind(replay_latency_ms)
        .bind(storage_bytes)
        .bind(notes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_bench(&self) -> anyhow::Result<Option<BenchRun>> {
        let row = sqlx::query_as::<_, BenchRun>(
            r#"
            SELECT id, label, events_per_sec, replay_latency_ms, storage_bytes, notes, created_at
            FROM bench_runs
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn ensure_stream_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO streams (stream_id)
            VALUES ($1)
            ON CONFLICT (stream_id) DO NOTHING
            "#,
        )
        .bind(stream_id)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn watermark_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: &str,
    ) -> anyhow::Result<StreamWatermark> {
        let row = sqlx::query_as::<_, (Option<DateTime<Utc>>, i64)>(
            r#"
            SELECT latest_event_time, latest_seq
            FROM streams
            WHERE stream_id = $1
            FOR UPDATE
            "#,
        )
        .bind(stream_id)
        .fetch_one(&mut **tx)
        .await?;

        Ok(StreamWatermark {
            last_event_time: row.0,
            last_seq: if row.1 == 0 { None } else { Some(row.1) },
        })
    }

    async fn find_by_event_id_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        stream_id: &str,
        event_id: &str,
    ) -> anyhow::Result<Option<EventRecord>> {
        let row = sqlx::query_as::<_, EventRow>(
            r#"
            SELECT stream_id, seq, event_id, event_time, ingest_time, payload, out_of_order
            FROM events
            WHERE stream_id = $1 AND event_id = $2
            "#,
        )
        .bind(stream_id)
        .bind(event_id)
        .fetch_optional(&mut **tx)
        .await?;
        Ok(row.map(Into::into))
    }
}

#[derive(Debug, sqlx::FromRow)]
struct EventRow {
    stream_id: String,
    seq: i64,
    event_id: String,
    event_time: DateTime<Utc>,
    ingest_time: DateTime<Utc>,
    payload: serde_json::Value,
    out_of_order: bool,
}

impl From<EventRow> for EventRecord {
    fn from(r: EventRow) -> Self {
        Self {
            stream_id: r.stream_id,
            seq: r.seq,
            event_id: r.event_id,
            event_time: r.event_time,
            ingest_time: r.ingest_time,
            payload: r.payload,
            out_of_order: r.out_of_order,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct StreamStatsRow {
    stream_id: String,
    event_count: i64,
    latest_seq: i64,
    latest_event_time: Option<DateTime<Utc>>,
    duplicate_count: i64,
    out_of_order_count: i64,
}

impl From<StreamStatsRow> for StreamStats {
    fn from(r: StreamStatsRow) -> Self {
        Self {
            stream_id: r.stream_id,
            event_count: r.event_count,
            latest_seq: if r.latest_seq == 0 {
                None
            } else {
                Some(r.latest_seq)
            },
            latest_event_time: r.latest_event_time,
            duplicate_count: r.duplicate_count,
            out_of_order_count: r.out_of_order_count,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct ReplayRow {
    replay_id: String,
    stream_id: String,
    from_time: DateTime<Utc>,
    to_time: DateTime<Utc>,
    speed: String,
    status: String,
    events_emitted: i64,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    error: Option<String>,
}

impl TryFrom<ReplayRow> for ReplayStatus {
    type Error = anyhow::Error;

    fn try_from(r: ReplayRow) -> Result<Self, Self::Error> {
        Ok(Self {
            replay_id: r.replay_id,
            stream_id: r.stream_id,
            from: r.from_time,
            to: r.to_time,
            speed: ReplaySpeed::parse(&r.speed).map_err(anyhow::Error::msg)?,
            status: parse_status(&r.status)?,
            events_emitted: r.events_emitted,
            started_at: r.started_at,
            finished_at: r.finished_at,
            error: r.error,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct Alert {
    pub id: i64,
    pub stream_id: String,
    pub alert_type: String,
    pub event_id: Option<String>,
    pub seq: Option<i64>,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct BenchRun {
    pub id: i64,
    pub label: String,
    pub events_per_sec: f64,
    pub replay_latency_ms: f64,
    pub storage_bytes: i64,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
}

fn speed_to_str(speed: ReplaySpeed) -> &'static str {
    match speed {
        ReplaySpeed::OneX => "1x",
        ReplaySpeed::TenX => "10x",
        ReplaySpeed::HundredX => "100x",
        ReplaySpeed::Max => "max",
    }
}

fn status_to_str(status: ReplayStatusKind) -> &'static str {
    match status {
        ReplayStatusKind::Pending => "pending",
        ReplayStatusKind::Running => "running",
        ReplayStatusKind::Completed => "completed",
        ReplayStatusKind::Failed => "failed",
        ReplayStatusKind::Cancelled => "cancelled",
    }
}

fn parse_status(raw: &str) -> anyhow::Result<ReplayStatusKind> {
    Ok(match raw {
        "pending" => ReplayStatusKind::Pending,
        "running" => ReplayStatusKind::Running,
        "completed" => ReplayStatusKind::Completed,
        "failed" => ReplayStatusKind::Failed,
        "cancelled" => ReplayStatusKind::Cancelled,
        other => anyhow::bail!("unknown replay status `{other}`"),
    })
}
