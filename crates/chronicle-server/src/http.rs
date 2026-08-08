use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chronicle_core::{IngestRequest, IngestResponse, StreamStats};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::ingest::IngestService;
use crate::store::EventStore;

#[derive(Clone)]
pub struct AppState {
    pub ingest: IngestService,
    pub store: EventStore,
    pub started_at: Instant,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/streams/:stream_id/events", post(ingest_event))
        .route("/v1/streams", get(list_streams))
        .route("/v1/streams/:stream_id", get(get_stream))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state))
}

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match sqlx::query("SELECT 1").execute(state.store.pool()).await {
        Ok(_) => (StatusCode::OK, Json(serde_json::json!({ "ready": true }))).into_response(),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "ready": false, "error": err.to_string() })),
        )
            .into_response(),
    }
}

async fn ingest_event(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
    Json(req): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, ApiError> {
    Ok(Json(state.ingest.ingest(&stream_id, req).await?))
}

async fn list_streams(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<StreamStats>>, ApiError> {
    Ok(Json(state.store.list_stream_stats().await?))
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(stream_id): Path<String>,
) -> Result<Json<StreamStats>, ApiError> {
    state
        .store
        .stream_stats(&stream_id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("stream `{stream_id}` not found")))
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}
