//! Shared domain types and correctness helpers for Chronicle.

mod correctness;
mod error;
mod event;

pub use correctness::{evaluate_event, CorrectnessDecision, CorrectnessOutcome, StreamWatermark};
pub use error::{ChronicleError, Result};
pub use event::{EventEnvelope, EventRecord, IngestRequest, IngestResponse, StreamId, StreamStats};
