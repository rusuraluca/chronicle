use sqlx::{postgres::PgPoolOptions, PgPool};

/// Thin Postgres handle used by the scaffold health/ready checks.
/// Append-only event APIs land in the ingest PR.
#[derive(Clone)]
pub struct EventStore {
    pool: PgPool,
}

impl EventStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        let sql = include_str!("../../../../migrations/001_init.sql");
        sqlx::raw_sql(sql).execute(&self.pool).await?;
        Ok(())
    }
}
