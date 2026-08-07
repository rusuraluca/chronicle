use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Logical stream identifier (e.g. `orders.prod`).
pub type StreamId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventEnvelope {
    /// Producer-supplied idempotency key (unique per stream).
    pub event_id: String,
    /// Business / event time used for ordering and replay pacing.
    pub event_time: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventRecord {
    pub stream_id: StreamId,
    /// Monotonic per-stream sequence assigned at durable append.
    pub seq: i64,
    pub event_id: String,
    pub event_time: DateTime<Utc>,
    pub ingest_time: DateTime<Utc>,
    pub payload: Value,
    pub out_of_order: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestRequest {
    pub event_id: String,
    pub event_time: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    pub stream_id: StreamId,
    pub seq: i64,
    pub event_id: String,
    pub duplicate: bool,
    pub out_of_order: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamStats {
    pub stream_id: StreamId,
    pub event_count: i64,
    pub latest_seq: Option<i64>,
    pub latest_event_time: Option<DateTime<Utc>>,
    pub duplicate_count: i64,
    pub out_of_order_count: i64,
}

impl EventEnvelope {
    pub fn new(event_id: impl Into<String>, event_time: DateTime<Utc>, payload: Value) -> Self {
        Self {
            event_id: event_id.into(),
            event_time,
            payload,
        }
    }

    pub fn with_random_id(event_time: DateTime<Utc>, payload: Value) -> Self {
        Self::new(Uuid::new_v4().to_string(), event_time, payload)
    }
}
