mod backend;
pub mod migrations;

pub use backend::PostgresBackend;

use pmp_mq_core::{MqError, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Configuration for Postgres backend
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PostgresConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://postgres:postgres@localhost/pmp_mq".to_string(),
            max_connections: 10,
        }
    }
}

/// Initialize a Postgres connection pool and run migrations
pub async fn create_pool(config: &PostgresConfig) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to connect to database: {}", e)))?;

    // Run migrations
    migrations::run_migrations(&pool).await?;

    Ok(pool)
}
