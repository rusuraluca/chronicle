use chronicle_core::{EventEnvelope, IngestRequest, IngestResponse};
use tracing::instrument;

use crate::store::EventStore;

#[derive(Clone)]
pub struct IngestService {
    store: EventStore,
}

impl IngestService {
    pub fn new(store: EventStore) -> Self {
        Self { store }
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

        self.store.append(stream_id, envelope).await
    }
}
