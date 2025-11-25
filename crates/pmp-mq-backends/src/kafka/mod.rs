mod backend;

pub use backend::KafkaBackend;

use rdkafka::ClientConfig;

/// Configuration for Kafka backend
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    /// Postgres connection for metadata storage
    pub postgres_url: String,
    pub max_connections: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            group_id: "pmp-mq-consumer".to_string(),
            postgres_url: "postgres://postgres:postgres@localhost/pmp_mq".to_string(),
            max_connections: 10,
        }
    }
}

/// Create a Kafka producer config
pub fn create_producer_config(brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "100000")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("batch.num.messages", "10000");
    config
}

/// Create a Kafka consumer config
pub fn create_consumer_config(brokers: &str, group_id: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000");
    config
}
