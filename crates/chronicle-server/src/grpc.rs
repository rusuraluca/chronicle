use chronicle_core::IngestRequest;
use chrono::{DateTime, Utc};
use tonic::{Request, Response, Status};

use crate::ingest::IngestService;

pub mod pb {
    tonic::include_proto!("chronicle.v1");
}

use pb::ingest_service_server::{IngestService as GrpcIngest, IngestServiceServer};
use pb::{
    IngestEventBatchRequest, IngestEventBatchResponse, IngestEventRequest, IngestEventResponse,
};

pub fn service(ingest: IngestService) -> IngestServiceServer<IngestGrpc> {
    IngestServiceServer::new(IngestGrpc { ingest })
}

#[derive(Clone)]
pub struct IngestGrpc {
    ingest: IngestService,
}

#[tonic::async_trait]
impl GrpcIngest for IngestGrpc {
    async fn ingest_event(
        &self,
        request: Request<IngestEventRequest>,
    ) -> Result<Response<IngestEventResponse>, Status> {
        let req = request.into_inner();
        let event_time = parse_event_time(&req.event_time)?;
        let payload = parse_payload(&req.payload_json)?;
        let resp = self
            .ingest
            .ingest(
                &req.stream_id,
                IngestRequest {
                    event_id: req.event_id,
                    event_time,
                    payload,
                },
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(IngestEventResponse {
            stream_id: resp.stream_id,
            seq: resp.seq,
            event_id: resp.event_id,
            duplicate: resp.duplicate,
            out_of_order: resp.out_of_order,
        }))
    }

    async fn ingest_event_batch(
        &self,
        request: Request<IngestEventBatchRequest>,
    ) -> Result<Response<IngestEventBatchResponse>, Status> {
        let batch = request.into_inner();
        let mut results = Vec::with_capacity(batch.events.len());
        for mut event in batch.events {
            if event.stream_id.is_empty() {
                event.stream_id = batch.stream_id.clone();
            }
            let resp = self.ingest_event(Request::new(event)).await?.into_inner();
            results.push(resp);
        }
        Ok(Response::new(IngestEventBatchResponse { results }))
    }
}

#[allow(clippy::result_large_err)]
fn parse_event_time(raw: &str) -> Result<DateTime<Utc>, Status> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Status::invalid_argument(format!("invalid event_time: {e}")))
}

#[allow(clippy::result_large_err)]
fn parse_payload(raw: &str) -> Result<serde_json::Value, Status> {
    if raw.trim().is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_str(raw)
        .map_err(|e| Status::invalid_argument(format!("invalid payload_json: {e}")))
}
