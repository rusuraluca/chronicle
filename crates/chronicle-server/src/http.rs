use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chronicle_core::{
    IngestRequest, IngestResponse, ReplayRequest, ReplaySpeed, ReplayStatus, StreamStats,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::ingest::IngestService;
use crate::replay::ReplayEngine;
use crate::store::{Alert, BenchRun, EventStore, RedisFanout};

#[derive(Clone)]
pub struct AppState {
    pub ingest: IngestService,
    pub replay: ReplayEngine,
    pub store: EventStore,
    pub redis: RedisFanout,
    pub started_at: Instant,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/streams/:stream_id/events", post(ingest_event))
        .route("/v1/streams", get(list_streams))
        .route("/v1/streams/:stream_id", get(get_stream))
        .route("/v1/replays", post(start_replay).get(list_replays))
        .route("/v1/replays/:replay_id", get(get_replay))
        .route("/v1/alerts", get(list_alerts))
        .route("/v1/status", get(system_status))
        .route("/v1/bench", get(get_bench).post(run_bench))
        .route("/metrics", get(metrics_handler))
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
    let resp = state.ingest.ingest(&stream_id, req).await?;
    Ok(Json(resp))
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

#[derive(Debug, Deserialize)]
pub struct StartReplayBody {
    pub stream_id: String,
    pub from: chrono::DateTime<chrono::Utc>,
    pub to: chrono::DateTime<chrono::Utc>,
    pub speed: String,
}

async fn start_replay(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartReplayBody>,
) -> Result<(StatusCode, Json<ReplayStatus>), ApiError> {
    let speed = ReplaySpeed::parse(&body.speed).map_err(ApiError::bad_request)?;
    let req = ReplayRequest {
        stream_id: body.stream_id,
        from: body.from,
        to: body.to,
        speed,
    };
    let status = state.replay.start(req).await?;
    Ok((StatusCode::ACCEPTED, Json(status)))
}

async fn list_replays(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ReplayStatus>>, ApiError> {
    Ok(Json(state.store.list_replays(50).await?))
}

async fn get_replay(
    State(state): State<Arc<AppState>>,
    Path(replay_id): Path<String>,
) -> Result<Json<ReplayStatus>, ApiError> {
    state
        .store
        .get_replay(&replay_id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("replay `{replay_id}` not found")))
}

async fn list_alerts(State(state): State<Arc<AppState>>) -> Result<Json<Vec<Alert>>, ApiError> {
    Ok(Json(state.store.recent_alerts(100).await?))
}

#[derive(Debug, Serialize)]
struct SystemStatus {
    uptime_secs: u64,
    streams: Vec<StreamStats>,
    active_replays: Vec<ReplayStatus>,
    recent_alerts: Vec<Alert>,
    storage_bytes: i64,
    redis_lengths: Vec<RedisLen>,
}

#[derive(Debug, Serialize)]
struct RedisLen {
    stream_id: String,
    xlen: i64,
}

async fn system_status(State(state): State<Arc<AppState>>) -> Result<Json<SystemStatus>, ApiError> {
    let streams = state.store.list_stream_stats().await?;
    let replays = state.store.list_replays(20).await?;
    let active_replays = replays
        .into_iter()
        .filter(|r| {
            matches!(
                r.status,
                chronicle_core::ReplayStatusKind::Pending
                    | chronicle_core::ReplayStatusKind::Running
            )
        })
        .collect::<Vec<_>>();
    let recent_alerts = state.store.recent_alerts(20).await?;
    let storage_bytes = state.store.storage_bytes().await?;
    let mut redis_lengths = Vec::new();
    for s in &streams {
        let xlen = state.redis.xlen(&s.stream_id).await.unwrap_or(0);
        redis_lengths.push(RedisLen {
            stream_id: s.stream_id.clone(),
            xlen,
        });
    }
    Ok(Json(SystemStatus {
        uptime_secs: state.started_at.elapsed().as_secs(),
        streams,
        active_replays,
        recent_alerts,
        storage_bytes,
        redis_lengths,
    }))
}

#[derive(Debug, Deserialize)]
struct BenchBody {
    #[serde(default = "default_bench_count")]
    count: usize,
    #[serde(default = "default_bench_stream")]
    stream_id: String,
}

fn default_bench_count() -> usize {
    1_000
}
fn default_bench_stream() -> String {
    "bench".into()
}

async fn run_bench(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BenchBody>,
) -> Result<Json<BenchRun>, ApiError> {
    let count = body.count.clamp(10, 50_000);
    let stream_id = body.stream_id;
    let start = Instant::now();
    let base = chrono::Utc::now();

    for i in 0..count {
        let req = IngestRequest {
            event_id: format!("bench-{i}"),
            event_time: base + chrono::Duration::milliseconds(i as i64),
            payload: serde_json::json!({ "n": i }),
        };
        state.ingest.ingest(&stream_id, req).await?;
    }
    let ingest_elapsed = start.elapsed();
    let events_per_sec = count as f64 / ingest_elapsed.as_secs_f64().max(1e-9);

    let replay_start = Instant::now();
    let from = base;
    let to = base + chrono::Duration::milliseconds(count as i64);
    let status = state
        .replay
        .start(ReplayRequest {
            stream_id: stream_id.clone(),
            from,
            to,
            speed: ReplaySpeed::Max,
        })
        .await?;

    // Wait briefly for max-speed replay to finish for small counts.
    for _ in 0..200 {
        if let Some(s) = state.store.get_replay(&status.replay_id).await? {
            if matches!(
                s.status,
                chronicle_core::ReplayStatusKind::Completed
                    | chronicle_core::ReplayStatusKind::Failed
            ) {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let replay_latency_ms = replay_start.elapsed().as_secs_f64() * 1000.0;
    let storage_bytes = state.store.storage_bytes().await?;
    state
        .store
        .record_bench(
            &format!("auto-{count}"),
            events_per_sec,
            replay_latency_ms,
            storage_bytes,
            Some("POST /v1/bench"),
        )
        .await?;

    let bench = state
        .store
        .latest_bench()
        .await?
        .ok_or_else(|| ApiError::internal("bench run missing after insert"))?;
    Ok(Json(bench))
}

async fn get_bench(State(state): State<Arc<AppState>>) -> Result<Json<Option<BenchRun>>, ApiError> {
    Ok(Json(state.store.latest_bench().await?))
}

async fn metrics_handler() -> impl IntoResponse {
    let body = crate::metrics::render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
    fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::internal(err.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}
