//! Shared domain types for Chronicle.

mod error;
mod event;

pub use error::{ChronicleError, Result};
pub use event::{EventEnvelope, EventRecord, IngestRequest, IngestResponse, StreamId, StreamStats};
