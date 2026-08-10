use std::path::PathBuf;
use std::time::Duration;

use chronicle_core::{IngestRequest, IngestResponse, ReplayStatus, StreamStats};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "chronicle",
    about = "Chronicle CLI — ingest, replay, and inspect streaming event logs",
    version
)]
struct Cli {
    /// Base URL of the Chronicle HTTP API
    #[arg(
        long,
        env = "CHRONICLE_URL",
        global = true,
        default_value = "http://127.0.0.1:8080"
    )]
    url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Ingest a single event (or generate one)
    Ingest {
        /// Stream id
        #[arg(long)]
        stream: String,
        /// Producer event id (defaults to a new UUID)
        #[arg(long)]
        event_id: Option<String>,
        /// Event time RFC3339 (defaults to now)
        #[arg(long)]
        event_time: Option<String>,
        /// JSON payload string
        #[arg(long, default_value = "{}")]
        payload: String,
        /// Read payload from file
        #[arg(long)]
        payload_file: Option<PathBuf>,
    },
    /// Start a replay from T1 to T2 at a given speed
    Replay {
        #[arg(long)]
        stream: String,
        /// Start time (RFC3339)
        #[arg(long)]
        from: String,
        /// End time (RFC3339)
        #[arg(long)]
        to: String,
        /// Speed: 1x | 10x | 100x | max
        #[arg(long, default_value = "1x")]
        speed: String,
        /// Poll until the replay completes
        #[arg(long)]
        wait: bool,
    },
    /// Show system / stream status
    Status {
        /// Optional stream id for a single stream
        #[arg(long)]
        stream: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    match cli.command {
        Commands::Ingest {
            stream,
            event_id,
            event_time,
            payload,
            payload_file,
        } => {
            let payload = if let Some(path) = payload_file {
                std::fs::read_to_string(path)?
            } else {
                payload
            };
            let payload: serde_json::Value = serde_json::from_str(&payload)?;
            let event_time = match event_time {
                Some(raw) => DateTime::parse_from_rfc3339(&raw)?.with_timezone(&Utc),
                None => Utc::now(),
            };
            let body = IngestRequest {
                event_id: event_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                event_time,
                payload,
            };
            let url = format!(
                "{}/v1/streams/{}/events",
                cli.url.trim_end_matches('/'),
                stream
            );
            let resp = client.post(&url).json(&body).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if !status.is_success() {
                anyhow::bail!("ingest failed ({status}): {text}");
            }
            let parsed: IngestResponse = serde_json::from_str(&text)?;
            println!("{}", serde_json::to_string_pretty(&parsed)?);
        }
        Commands::Replay {
            stream,
            from,
            to,
            speed,
            wait,
        } => {
            let url = format!("{}/v1/replays", cli.url.trim_end_matches('/'));
            let body = serde_json::json!({
                "stream_id": stream,
                "from": from,
                "to": to,
                "speed": speed,
            });
            let resp = client.post(&url).json(&body).send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            if !status.is_success() {
                anyhow::bail!("replay failed ({status}): {text}");
            }
            let mut replay: ReplayStatus = serde_json::from_str(&text)?;
            println!("{}", serde_json::to_string_pretty(&replay)?);

            if wait {
                let get_url = format!(
                    "{}/v1/replays/{}",
                    cli.url.trim_end_matches('/'),
                    replay.replay_id
                );
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let r = client.get(&get_url).send().await?;
                    replay = r.json().await?;
                    match replay.status {
                        chronicle_core::ReplayStatusKind::Completed
                        | chronicle_core::ReplayStatusKind::Failed
                        | chronicle_core::ReplayStatusKind::Cancelled => {
                            println!("{}", serde_json::to_string_pretty(&replay)?);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }
        Commands::Status { stream } => {
            if let Some(stream) = stream {
                let url = format!("{}/v1/streams/{}", cli.url.trim_end_matches('/'), stream);
                let resp = client.get(&url).send().await?;
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    anyhow::bail!("status failed ({status}): {text}");
                }
                let parsed: StreamStats = serde_json::from_str(&text)?;
                println!("{}", serde_json::to_string_pretty(&parsed)?);
            } else {
                let url = format!("{}/v1/status", cli.url.trim_end_matches('/'));
                let resp = client.get(&url).send().await?;
                let status = resp.status();
                let text = resp.text().await?;
                if !status.is_success() {
                    anyhow::bail!("status failed ({status}): {text}");
                }
                let parsed: serde_json::Value = serde_json::from_str(&text)?;
                println!("{}", serde_json::to_string_pretty(&parsed)?);
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}
