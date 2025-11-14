mod api;
mod config;
mod delivery;

use anyhow::Result;
use config::Config;
use std::sync::Arc;
use tokio::signal;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pmp_mq_server=debug,tower_http=debug".into()),
        )
        .init();

    info!("Starting PMP Message Queue Server");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: backend={}", config.backend_type);

    // Initialize backend based on configuration
    let backend: Arc<dyn pmp_mq_core::Backend> = match config.backend_type.as_str() {
        "postgres" => {
            info!("Initializing Postgres backend");
            let pool = pmp_mq_backends::postgres::create_pool(&config.postgres).await?;
            Arc::new(pmp_mq_backends::PostgresBackend::new(pool))
        }
        "kafka" => {
            info!("Initializing Kafka backend");
            let metadata_pool =
                pmp_mq_backends::postgres::create_pool(&pmp_mq_backends::postgres::PostgresConfig {
                    database_url: config.kafka.postgres_url.clone(),
                    max_connections: config.kafka.max_connections,
                })
                .await?;

            let kafka_config = pmp_mq_backends::kafka::create_producer_config(&config.kafka.brokers);
            Arc::new(
                pmp_mq_backends::KafkaBackend::new(&kafka_config, metadata_pool).await?,
            )
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid backend type: {}",
                config.backend_type
            ))
        }
    };

    // Health check
    backend.health_check().await?;
    info!("Backend health check passed");

    // Start webhook delivery worker
    let delivery_worker = tokio::spawn({
        let backend = Arc::clone(&backend);
        let config = config.clone();
        async move {
            delivery::run_delivery_worker(backend, config.delivery).await;
        }
    });

    // Start HTTP server
    let app = api::create_app(backend);
    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Server listening on {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Wait for delivery worker to finish
    delivery_worker.abort();

    info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}
