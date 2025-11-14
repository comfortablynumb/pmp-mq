use anyhow::Result;
use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_backend_type")]
    pub backend_type: String,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub postgres: pmp_mq_backends::postgres::PostgresConfig,

    #[serde(default)]
    pub kafka: pmp_mq_backends::kafka::KafkaConfig,

    #[serde(default)]
    pub sqs: pmp_mq_backends::sqs::SqsConfig,

    #[serde(default)]
    pub delivery: DeliveryConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_worker_interval_secs")]
    pub worker_interval_secs: u64,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_max_concurrent_deliveries")]
    pub max_concurrent_deliveries: usize,

    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            worker_interval_secs: default_worker_interval_secs(),
            batch_size: default_batch_size(),
            max_concurrent_deliveries: default_max_concurrent_deliveries(),
            request_timeout_secs: default_request_timeout_secs(),
        }
    }
}

fn default_backend_type() -> String {
    env::var("BACKEND_TYPE").unwrap_or_else(|_| "postgres".to_string())
}

fn default_host() -> String {
    env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string())
}

fn default_port() -> u16 {
    env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080)
}

fn default_worker_interval_secs() -> u64 {
    5
}

fn default_batch_size() -> usize {
    100
}

fn default_max_concurrent_deliveries() -> usize {
    50
}

fn default_request_timeout_secs() -> u64 {
    30
}

impl Config {
    pub fn load() -> Result<Self> {
        // Try to load from environment variables with a config file fallback
        let config_file = env::var("CONFIG_FILE").unwrap_or_else(|_| "config.toml".to_string());

        let mut builder = config::Config::builder()
            .add_source(config::File::with_name(&config_file).required(false))
            .add_source(
                config::Environment::with_prefix("PMP_MQ")
                    .separator("__")
                    .try_parsing(true),
            );

        // Override backend type from env
        if let Ok(backend_type) = env::var("BACKEND_TYPE") {
            builder = builder.set_override("backend_type", backend_type)?;
        }

        // Override database URL from env
        if let Ok(database_url) = env::var("DATABASE_URL") {
            builder = builder.set_override("postgres.database_url", database_url)?;
        }

        let config = builder.build()?;
        Ok(config.try_deserialize()?)
    }
}
