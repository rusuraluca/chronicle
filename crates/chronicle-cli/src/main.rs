use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "chronicle",
    about = "Chronicle CLI — ingest, replay, and inspect streaming event logs",
    version
)]
struct Cli {
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
    /// Show API health (full status arrives in later PRs)
    Status,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Status => {
            let url = format!("{}/healthz", cli.url.trim_end_matches('/'));
            let body = reqwest::get(&url).await?.error_for_status()?.text().await?;
            println!("{body}");
        }
    }
    Ok(())
}
