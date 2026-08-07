use thiserror::Error;

pub type Result<T> = std::result::Result<T, ChronicleError>;

#[derive(Debug, Error)]
pub enum ChronicleError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("duplicate event_id `{event_id}` on stream `{stream_id}`")]
    Duplicate {
        stream_id: String,
        event_id: String,
        existing_seq: i64,
    },

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("internal error: {0}")]
    Internal(String),
}
