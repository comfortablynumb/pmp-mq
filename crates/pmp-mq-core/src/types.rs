use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an event/message to be published
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique identifier for the event
    pub id: Uuid,

    /// Topic to which the event belongs
    pub topic: String,

    /// Event payload (arbitrary JSON)
    pub payload: serde_json::Value,

    /// Optional metadata
    #[serde(default)]
    pub metadata: serde_json::Value,

    /// Timestamp when the event was created
    pub created_at: DateTime<Utc>,

    /// Optional event type/schema identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
}

impl Event {
    pub fn new(topic: String, payload: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            payload,
            metadata: serde_json::Value::Null,
            created_at: Utc::now(),
            event_type: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_event_type(mut self, event_type: String) -> Self {
        self.event_type = Some(event_type);
        self
    }
}

/// Represents a topic in the message queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    /// Unique identifier for the topic
    pub id: Uuid,

    /// Topic name (must be unique)
    pub name: String,

    /// Optional description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Timestamp when created
    pub created_at: DateTime<Utc>,

    /// Configuration options
    #[serde(default)]
    pub config: TopicConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Retention period in seconds (None = infinite)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_seconds: Option<i64>,

    /// Maximum message size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_size: Option<usize>,

    /// Whether to store event data in the database
    /// Default: false (events are only tracked for delivery, not stored)
    #[serde(default = "default_store_events")]
    pub store_events: bool,
}

fn default_store_events() -> bool {
    false
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            retention_seconds: None,
            max_message_size: None,
            store_events: default_store_events(),
        }
    }
}

/// Represents a subscription to a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique identifier for the subscription
    pub id: Uuid,

    /// Name of the subscription
    pub name: String,

    /// Topic being subscribed to
    pub topic_name: String,

    /// Filter expression (optional, for future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Timestamp when created
    pub created_at: DateTime<Utc>,

    /// Configuration options
    #[serde(default)]
    pub config: SubscriptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SubscriptionConfig {
    /// Acknowledgment timeout in seconds
    #[serde(default = "default_ack_timeout")]
    pub ack_timeout_seconds: i64,

    /// Maximum delivery attempts
    #[serde(default = "default_max_delivery_attempts")]
    pub max_delivery_attempts: i32,
}

fn default_ack_timeout() -> i64 {
    300 // 5 minutes
}

fn default_max_delivery_attempts() -> i32 {
    5
}

/// Represents a client that receives events via webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Client {
    /// Unique identifier for the client
    pub id: Uuid,

    /// Client name
    pub name: String,

    /// Webhook URL to deliver events to
    pub webhook_url: String,

    /// Subscription this client is registered to
    pub subscription_name: String,

    /// Optional authentication headers
    #[serde(default)]
    pub auth_headers: std::collections::HashMap<String, String>,

    /// Timestamp when registered
    pub created_at: DateTime<Utc>,

    /// Whether the client is active
    #[serde(default = "default_true")]
    pub active: bool,
}

fn default_true() -> bool {
    true
}

/// Represents the delivery status of an event to a client
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeliveryStatus {
    Pending,
    InProgress,
    Success,
    Failed,
    DeadLetter,
}

/// Record of event delivery attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryAttempt {
    pub id: Uuid,
    pub event_id: Uuid,
    pub client_id: Uuid,
    pub attempt_number: i32,
    pub status: DeliveryStatus,
    pub attempted_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub http_status_code: Option<u16>,
}

/// Request to publish an event
#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub topic: String,
    pub payload: serde_json::Value,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub event_type: Option<String>,
}

/// Request to create a topic
#[derive(Debug, Clone, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub config: TopicConfig,
}

/// Request to create a subscription
#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    pub name: String,
    pub topic_name: String,
    pub filter: Option<String>,
    #[serde(default)]
    pub config: SubscriptionConfig,
}

/// Request to register a client
#[derive(Debug, Deserialize)]
pub struct RegisterClientRequest {
    pub name: String,
    pub webhook_url: String,
    pub subscription_name: String,
    #[serde(default)]
    pub auth_headers: std::collections::HashMap<String, String>,
}

/// Response for successful event publication
#[derive(Debug, Serialize)]
pub struct PublishResponse {
    pub event_id: Uuid,
    pub topic: String,
    pub published_at: DateTime<Utc>,
}

/// Request to publish multiple events in batch
#[derive(Debug, Deserialize)]
pub struct BatchPublishRequest {
    pub events: Vec<PublishRequest>,
}

/// Response for batch event publication
#[derive(Debug, Serialize)]
pub struct BatchPublishResponse {
    pub published: Vec<PublishResponse>,
    pub failed: Vec<BatchPublishError>,
    pub total: usize,
    pub success_count: usize,
    pub failure_count: usize,
}

/// Error for individual event in batch
#[derive(Debug, Serialize)]
pub struct BatchPublishError {
    pub index: usize,
    pub topic: String,
    pub error: String,
}

/// System metrics
#[derive(Debug, Serialize)]
pub struct SystemMetrics {
    pub topics_count: i64,
    pub subscriptions_count: i64,
    pub clients_count: i64,
    pub active_clients_count: i64,
    pub pending_deliveries: i64,
    pub failed_deliveries: i64,
    pub dead_letter_count: i64,
}

/// Topic metrics
#[derive(Debug, Serialize)]
pub struct TopicMetrics {
    pub topic_name: String,
    pub total_events: i64,
    pub total_deliveries: i64,
    pub pending_deliveries: i64,
    pub successful_deliveries: i64,
    pub failed_deliveries: i64,
}
