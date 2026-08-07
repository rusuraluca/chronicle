//! Integration tests against Postgres + Redis.
//!
//! Requires:
//!   DATABASE_URL=postgres://chronicle:chronicle@127.0.0.1:5432/chronicle
//!   REDIS_URL=redis://127.0.0.1:6379
//!
//! Skipped automatically when those services are unreachable.

use std::time::{Duration, Instant};

use chronicle_core::{EventEnvelope, IngestRequest, ReplayRequest, ReplaySpeed, ReplayStatusKind};
use chronicle_server::http::{self, AppState};
use chronicle_server::ingest::IngestService;
use chronicle_server::replay::ReplayEngine;
use chronicle_server::store::{EventStore, RedisFanout};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use serde_json::json;
use tower::ServiceExt;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://chronicle:chronicle@127.0.0.1:5432/chronicle".into())
}

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

fn require_integration() -> bool {
    matches!(
        std::env::var("CHRONICLE_REQUIRE_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

async fn try_connect() -> Option<(EventStore, RedisFanout)> {
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
    let redis = match RedisFanout::connect(&redis_url()).await {
        Ok(r) => r,
        Err(err) => {
            if require_integration() {
                panic!("redis unavailable (required): {err}");
            }
            eprintln!("skipping integration tests: redis unavailable ({err})");
            return None;
        }
    };
    Some((store, redis))
}

async fn build_app(store: EventStore, redis: RedisFanout) -> axum::Router {
    let ingest = IngestService::new(store.clone(), redis.clone());
    let replay = ReplayEngine::new(store.clone(), redis.clone());
    http::router(AppState {
        ingest,
        replay,
        store,
        redis,
        started_at: Instant::now(),
    })
}

fn unique_stream(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

#[tokio::test]
async fn healthz_ok() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let app = build_app(store, redis).await;
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/healthz")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn ordered_sequence_assignment() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let ingest = IngestService::new(store.clone(), redis);
    let stream = unique_stream("seq");
    let t0 = Utc.timestamp_opt(1_700_000_000, 0).unwrap();

    for i in 0..5 {
        let resp = ingest
            .ingest(
                &stream,
                IngestRequest {
                    event_id: format!("e{i}"),
                    event_time: t0 + ChronoDuration::seconds(i),
                    payload: json!({ "i": i }),
                },
            )
            .await
            .unwrap();
        assert!(!resp.duplicate);
        assert_eq!(resp.seq, i + 1);
    }

    let stats = store.stream_stats(&stream).await.unwrap().unwrap();
    assert_eq!(stats.event_count, 5);
    assert_eq!(stats.latest_seq, Some(5));
}

#[tokio::test]
async fn duplicate_ingest_is_idempotent() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let ingest = IngestService::new(store.clone(), redis);
    let stream = unique_stream("dup");
    let t0 = Utc::now();
    let req = IngestRequest {
        event_id: "same-id".into(),
        event_time: t0,
        payload: json!({ "v": 1 }),
    };

    let first = ingest.ingest(&stream, req.clone()).await.unwrap();
    assert!(!first.duplicate);
    assert_eq!(first.seq, 1);

    let second = ingest.ingest(&stream, req).await.unwrap();
    assert!(second.duplicate);
    assert_eq!(second.seq, 1);

    let stats = store.stream_stats(&stream).await.unwrap().unwrap();
    assert_eq!(stats.event_count, 1);
    assert_eq!(stats.duplicate_count, 1);

    let alerts = store.recent_alerts(20).await.unwrap();
    assert!(alerts
        .iter()
        .any(|a| a.stream_id == stream && a.alert_type == "duplicate"));
}

#[tokio::test]
async fn out_of_order_flagged_and_alerted() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let ingest = IngestService::new(store.clone(), redis);
    let stream = unique_stream("ooo");
    let t_late = Utc.timestamp_opt(1_700_000_100, 0).unwrap();
    let t_early = Utc.timestamp_opt(1_700_000_050, 0).unwrap();

    ingest
        .ingest(
            &stream,
            IngestRequest {
                event_id: "late".into(),
                event_time: t_late,
                payload: json!({}),
            },
        )
        .await
        .unwrap();

    let ooo = ingest
        .ingest(
            &stream,
            IngestRequest {
                event_id: "early".into(),
                event_time: t_early,
                payload: json!({}),
            },
        )
        .await
        .unwrap();
    assert!(ooo.out_of_order);
    assert_eq!(ooo.seq, 2);

    let record = store
        .find_by_event_id(&stream, "early")
        .await
        .unwrap()
        .unwrap();
    assert!(record.out_of_order);

    let stats = store.stream_stats(&stream).await.unwrap().unwrap();
    assert_eq!(stats.out_of_order_count, 1);

    let alerts = store.recent_alerts(20).await.unwrap();
    assert!(alerts
        .iter()
        .any(|a| a.stream_id == stream && a.alert_type == "out_of_order"));
}

#[tokio::test]
async fn replay_range_order_and_max_speed_completion() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let replay = ReplayEngine::new(store.clone(), redis);
    let stream = unique_stream("replay");
    let base = Utc.timestamp_opt(1_700_001_000, 0).unwrap();

    // Insert deliberately shuffled event_times; durable seq still monotonic.
    for (id, offset) in [("a", 2i64), ("b", 0), ("c", 1)] {
        store
            .append(
                &stream,
                EventEnvelope {
                    event_id: id.into(),
                    event_time: base + ChronoDuration::seconds(offset),
                    payload: json!({ "id": id }),
                },
            )
            .await
            .unwrap();
    }

    let events = store
        .events_in_range(
            &stream,
            base - ChronoDuration::seconds(1),
            base + ChronoDuration::seconds(10),
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events
            .iter()
            .map(|e| e.event_id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "c", "a"]
    );

    let status = replay
        .start(ReplayRequest {
            stream_id: stream.clone(),
            from: base - ChronoDuration::seconds(1),
            to: base + ChronoDuration::seconds(10),
            speed: ReplaySpeed::Max,
        })
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let current = store.get_replay(&status.replay_id).await.unwrap().unwrap();
        if current.status == ReplayStatusKind::Completed {
            assert_eq!(current.events_emitted, 3);
            break;
        }
        if current.status == ReplayStatusKind::Failed {
            panic!("replay failed: {:?}", current.error);
        }
        if Instant::now() > deadline {
            panic!("replay did not complete in time: {:?}", current.status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn rest_ingest_roundtrip() {
    let Some((store, redis)) = try_connect().await else {
        return;
    };
    let app = build_app(store, redis).await;
    let stream = unique_stream("http");
    let body = serde_json::json!({
        "event_id": "http-1",
        "event_time": Utc::now().to_rfc3339(),
        "payload": { "ok": true }
    });
    let response = app
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
    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
