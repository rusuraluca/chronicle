//! Integration tests against Postgres.
//!
//! Requires DATABASE_URL. Skipped when unreachable unless
//! CHRONICLE_REQUIRE_INTEGRATION=1.

use std::time::Instant;

use chronicle_core::IngestRequest;
use chronicle_server::http::{self, AppState};
use chronicle_server::ingest::IngestService;
use chronicle_server::store::EventStore;
use chrono::{TimeZone, Utc};
use serde_json::json;
use tower::ServiceExt;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://chronicle:chronicle@127.0.0.1:5432/chronicle".into())
}

fn require_integration() -> bool {
    matches!(
        std::env::var("CHRONICLE_REQUIRE_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

async fn try_connect() -> Option<EventStore> {
    let store = match EventStore::connect(&database_url()).await {
        Ok(s) => s,
        Err(err) => {
            if require_integration() {
                panic!("postgres unavailable (required): {err}");
            }
            eprintln!("skipping integration tests: postgres unavailable ({err})");
            return None;
        }
    };
    if let Err(err) = store.migrate().await {
        if require_integration() {
            panic!("migrate failed (required): {err}");
        }
        eprintln!("skipping integration tests: migrate failed ({err})");
        return None;
    }
    Some(store)
}

async fn build_app(store: EventStore) -> axum::Router {
    let ingest = IngestService::new(store.clone());
    http::router(AppState {
        ingest,
        store,
        started_at: Instant::now(),
    })
}

fn unique_stream(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

#[tokio::test]
async fn healthz_ok() {
    let Some(store) = try_connect().await else {
        return;
    };
    let app = build_app(store).await;
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn ordered_sequence_assignment() {
    let Some(store) = try_connect().await else {
        return;
    };
    let stream = unique_stream("seq");
    let ingest = IngestService::new(store);
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    for i in 0..3 {
        let resp = ingest
            .ingest(
                &stream,
                IngestRequest {
                    event_id: format!("e{i}"),
                    event_time: t0 + chrono::Duration::seconds(i),
                    payload: json!({ "i": i }),
                },
            )
            .await
            .unwrap();
        assert_eq!(resp.seq, i + 1);
        assert!(!resp.duplicate);
        assert!(!resp.out_of_order);
    }
}

#[tokio::test]
async fn duplicate_ingest_is_idempotent() {
    let Some(store) = try_connect().await else {
        return;
    };
    let stream = unique_stream("dup");
    let ingest = IngestService::new(store.clone());
    let req = IngestRequest {
        event_id: "same".into(),
        event_time: Utc::now(),
        payload: json!({}),
    };
    let a = ingest.ingest(&stream, req.clone()).await.unwrap();
    let b = ingest.ingest(&stream, req).await.unwrap();
    assert_eq!(a.seq, b.seq);
    assert!(b.duplicate);
    let stats = store.stream_stats(&stream).await.unwrap().unwrap();
    assert_eq!(stats.event_count, 1);
    assert_eq!(stats.duplicate_count, 1);
}

#[tokio::test]
async fn out_of_order_flagged_and_alerted() {
    let Some(store) = try_connect().await else {
        return;
    };
    let stream = unique_stream("ooo");
    let ingest = IngestService::new(store.clone());
    let t_hi = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
    let t_lo = Utc.timestamp_opt(1_700_000_050, 0).unwrap();
    ingest
        .ingest(
            &stream,
            IngestRequest {
                event_id: "hi".into(),
                event_time: t_hi,
                payload: json!({}),
            },
        )
        .await
        .unwrap();
    let ooo = ingest
        .ingest(
            &stream,
            IngestRequest {
                event_id: "lo".into(),
                event_time: t_lo,
                payload: json!({}),
            },
        )
        .await
        .unwrap();
    assert!(ooo.out_of_order);
    let stats = store.stream_stats(&stream).await.unwrap().unwrap();
    assert_eq!(stats.out_of_order_count, 1);
    let alerts = store.recent_alerts(10).await.unwrap();
    assert!(alerts
        .iter()
        .any(|a| a.stream_id == stream && a.alert_type == "out_of_order"));
}

#[tokio::test]
async fn rest_ingest_roundtrip() {
    let Some(store) = try_connect().await else {
        return;
    };
    let stream = unique_stream("rest");
    let app = build_app(store).await;
    let body = serde_json::json!({
        "event_id": "r1",
        "event_time": "2024-01-01T00:00:00Z",
        "payload": {"ok": true}
    });
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v1/streams/{stream}/events"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
}
