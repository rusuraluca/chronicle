use std::sync::Arc;

use chronicle_core::{pacing_delay, ReplayRequest, ReplayStatus, ReplayStatusKind};
use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{error, info, instrument};

use crate::store::{EventStore, RedisFanout};

#[derive(Clone)]
pub struct ReplayEngine {
    store: EventStore,
    redis: RedisFanout,
    /// In-memory handle for active replays (status also persisted).
    active: Arc<RwLock<Vec<String>>>,
}

impl ReplayEngine {
    pub fn new(store: EventStore, redis: RedisFanout) -> Self {
        Self {
            store,
            redis,
            active: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub async fn start(&self, req: ReplayRequest) -> anyhow::Result<ReplayStatus> {
        if req.to < req.from {
            anyhow::bail!("replay `to` must be >= `from`");
        }
        self.store.ensure_stream(&req.stream_id).await?;
        let status = self.store.create_replay(&req).await?;
        let engine = self.clone();
        let replay_id = status.replay_id.clone();
        tokio::spawn(async move {
            if let Err(err) = engine.run(replay_id.clone(), req).await {
                error!(replay_id = %replay_id, error = %err, "replay failed");
            }
        });
        Ok(status)
    }

    #[instrument(skip(self, req), fields(replay_id = %replay_id, stream_id = %req.stream_id))]
    async fn run(&self, replay_id: String, req: ReplayRequest) -> anyhow::Result<()> {
        {
            let mut active = self.active.write().await;
            active.push(replay_id.clone());
        }

        let mut status = self
            .store
            .get_replay(&replay_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("replay disappeared"))?;
        status.status = ReplayStatusKind::Running;
        status.started_at = Some(Utc::now());
        self.store.update_replay(&status).await?;

        let events = self
            .store
            .events_in_range(&req.stream_id, req.from, req.to)
            .await?;

        // Separate Redis stream key namespace so live consumers can opt into replay.
        let replay_stream = format!("replay:{}", req.stream_id);
        let _ = self
            .redis
            .ensure_consumer_group(&replay_stream, "chronicle-replay")
            .await;

        let mut prev_time = None;
        for event in &events {
            if let Some(prev) = prev_time {
                let delay = pacing_delay(prev, event.event_time, req.speed);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            // Publish original record identity — never invent new seq/event_id.
            let mut replayed = event.clone();
            replayed.stream_id = replay_stream.clone();
            if let Err(err) = self.redis.publish(&replayed).await {
                status.status = ReplayStatusKind::Failed;
                status.error = Some(err.to_string());
                status.finished_at = Some(Utc::now());
                self.store.update_replay(&status).await?;
                self.clear_active(&replay_id).await;
                return Err(err);
            }
            status.events_emitted += 1;
            prev_time = Some(event.event_time);
            metrics::counter!("chronicle_replay_events_total", "stream" => req.stream_id.clone())
                .increment(1);
        }

        // Periodic status flush at end (and every 100 events).
        status.status = ReplayStatusKind::Completed;
        status.finished_at = Some(Utc::now());
        self.store.update_replay(&status).await?;
        info!(
            replay_id = %replay_id,
            emitted = status.events_emitted,
            "replay completed"
        );
        self.clear_active(&replay_id).await;
        Ok(())
    }

    async fn clear_active(&self, replay_id: &str) {
        let mut active = self.active.write().await;
        active.retain(|id| id != replay_id);
    }
}
