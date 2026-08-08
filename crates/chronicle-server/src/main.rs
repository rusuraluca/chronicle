use std::time::Instant;

use chronicle_server::config::Config;
use chronicle_server::http::{self, AppState};
use chronicle_server::ingest::IngestService;
use chronicle_server::store::EventStore;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config = Config::from_env()?;
    info!(?config.http_addr, "starting chronicle-server");

    let store = EventStore::connect(&config.database_url).await?;
    store.migrate().await?;
    info!("database migrations applied");

    let ingest = IngestService::new(store.clone());
    let state = AppState {
        ingest,
        store,
        started_at: Instant::now(),
    };

    let app = http::router(state);
    let listener = TcpListener::bind(config.http_addr).await?;
    info!(addr = %config.http_addr, "HTTP listening");
    axum::serve(listener, app).await?;
    Ok(())
}
