mod backend;

pub use backend::SqsBackend;

/// Configuration for SQS backend
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SqsConfig {
    /// AWS Region (e.g., "us-east-1")
    pub region: String,

    /// Queue name prefix for topic queues
    #[serde(default = "default_queue_prefix")]
    pub queue_prefix: String,

    /// Postgres connection for metadata storage
    pub postgres_url: String,

    pub max_connections: u32,
}

fn default_queue_prefix() -> String {
    "pmp-mq-".to_string()
}

impl Default for SqsConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            queue_prefix: default_queue_prefix(),
            postgres_url: "postgres://postgres:postgres@localhost/pmp_mq".to_string(),
            max_connections: 10,
        }
    }
}

/// Create AWS SQS client
pub async fn create_sqs_client(region: &str) -> aws_sdk_sqs::Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;

    aws_sdk_sqs::Client::new(&config)
}
