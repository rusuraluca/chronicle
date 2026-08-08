//! Shared domain types and correctness helpers for Chronicle.
//!
//! The server and CLI both depend on this crate so event identity,
//! ordering, and duplicate / out-of-order rules stay consistent.

mod correctness;
mod error;
mod event;
mod replay;

pub use correctness::{evaluate_event, CorrectnessDecision, CorrectnessOutcome, StreamWatermark};
pub use error::{ChronicleError, Result};
pub use event::{EventEnvelope, EventRecord, IngestRequest, IngestResponse, StreamId, StreamStats};
pub use replay::{pacing_delay, ReplayRequest, ReplaySpeed, ReplayStatus, ReplayStatusKind};
