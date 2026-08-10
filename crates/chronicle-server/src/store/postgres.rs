use chronicle_core::{
    evaluate_event, CorrectnessDecision, EventEnvelope, EventRecord, IngestResponse, StreamStats,
    StreamWatermark,
};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};

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
        }

        tx.commit().await?;

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
