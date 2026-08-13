use std::time::Instant;

use chronicle_server::config::Config;
use chronicle_server::grpc;
use chronicle_server::http::{self, AppState};
use chronicle_server::ingest::IngestService;
use chronicle_server::store::{EventStore, RedisFanout};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config = Config::from_env()?;
    info!(?config.http_addr, ?config.grpc_addr, "starting chronicle-server");

    let store = EventStore::connect(&config.database_url).await?;
    store.migrate().await?;
    info!("database migrations applied");

    let redis = match RedisFanout::connect(&config.redis_url).await {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "redis unavailable at startup; retrying once");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            RedisFanout::connect(&config.redis_url).await?
        }
    };

    let ingest = IngestService::new(store.clone(), redis);
    let state = AppState {
        ingest: ingest.clone(),
        store,
        started_at: Instant::now(),
    };

    let http_app = http::router(state);
    let http_listener = TcpListener::bind(config.http_addr).await?;
    info!(addr = %config.http_addr, "HTTP listening");

    let grpc_addr = config.grpc_addr;
    let grpc_svc = grpc::service(ingest);
    info!(addr = %grpc_addr, "gRPC listening");

    let http_server = async move {
        axum::serve(http_listener, http_app)
            .await
            .map_err(anyhow::Error::from)
    };
    let grpc_server = async move {
        tonic::transport::Server::builder()
            .add_service(grpc_svc)
            .serve(grpc_addr)
            .await
            .map_err(anyhow::Error::from)
    };

    tokio::select! {
        res = http_server => res?,
        res = grpc_server => res?,
    }
    Ok(())
}
