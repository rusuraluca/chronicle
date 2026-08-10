use chronicle_core::{EventEnvelope, IngestRequest, IngestResponse};
use tracing::instrument;

use crate::store::{EventStore, RedisFanout};

#[derive(Clone)]
pub struct IngestService {
    store: EventStore,
    redis: RedisFanout,
}

impl IngestService {
    pub fn new(store: EventStore, redis: RedisFanout) -> Self {
        Self { store, redis }
    }

    #[instrument(skip(self, req), fields(stream_id = %stream_id, event_id = %req.event_id))]
    pub async fn ingest(
        &self,
        stream_id: &str,
        req: IngestRequest,
    ) -> anyhow::Result<IngestResponse> {
        if stream_id.trim().is_empty() {
            anyhow::bail!("stream_id must not be empty");
        }
        if req.event_id.trim().is_empty() {
            anyhow::bail!("event_id must not be empty");
        }

        let envelope = EventEnvelope {
            event_id: req.event_id,
            event_time: req.event_time,
            payload: req.payload,
        };

        let response = self.store.append(stream_id, envelope.clone()).await?;

        if !response.duplicate {
            // Fan-out only newly accepted events. Replays read from Postgres.
            if let Some(record) = self
                .store
                .find_by_event_id(stream_id, &response.event_id)
                .await?
            {
                if let Err(err) = self.redis.publish(&record).await {
                    tracing::warn!(error = %err, "redis publish failed; durable write succeeded");
                }
            }
        }

        Ok(response)
    }
}
