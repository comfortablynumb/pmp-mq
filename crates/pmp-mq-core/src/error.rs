use thiserror::Error;

#[derive(Error, Debug)]
pub enum MqError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),

    #[error("Client not found: {0}")]
    ClientNotFound(String),

    #[error("Backend error: {0}")]
    BackendError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    #[error("Webhook delivery failed: {0}")]
    WebhookError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, MqError>;
