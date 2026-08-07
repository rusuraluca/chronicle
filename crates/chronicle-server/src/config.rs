use std::env;
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://chronicle:chronicle@127.0.0.1:5432/chronicle".into());
        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
        let http_addr = env::var("CHRONICLE_HTTP_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".into())
            .parse()?;
        let grpc_addr = env::var("CHRONICLE_GRPC_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:50051".into())
            .parse()?;

        Ok(Self {
            database_url,
            redis_url,
            http_addr,
            grpc_addr,
        })
    }
}
