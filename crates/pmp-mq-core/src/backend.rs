use crate::{
    error::Result, Client, CreateSubscriptionRequest, CreateTopicRequest, DeliveryAttempt,
    DeliveryStatus, Event, RegisterClientRequest, Subscription, Topic,
};
use async_trait::async_trait;
use uuid::Uuid;

/// Backend trait that abstracts different message queue implementations
#[async_trait]
pub trait Backend: Send + Sync {
    // ===== Topic Management =====

    /// Create a new topic
    async fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic>;

    /// Get a topic by name
    async fn get_topic(&self, name: &str) -> Result<Option<Topic>>;

    /// List all topics
    async fn list_topics(&self) -> Result<Vec<Topic>>;

    /// Delete a topic
    async fn delete_topic(&self, name: &str) -> Result<()>;

    // ===== Subscription Management =====

    /// Create a new subscription
    async fn create_subscription(&self, request: CreateSubscriptionRequest) -> Result<Subscription>;

    /// Get a subscription by name
    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>>;

    /// List subscriptions for a topic
    async fn list_subscriptions(&self, topic_name: Option<&str>) -> Result<Vec<Subscription>>;

    /// Delete a subscription
    async fn delete_subscription(&self, name: &str) -> Result<()>;

    // ===== Client Management =====

    /// Register a new client for a subscription
    async fn register_client(&self, request: RegisterClientRequest) -> Result<Client>;

    /// Get a client by ID
    async fn get_client(&self, id: Uuid) -> Result<Option<Client>>;

    /// List clients for a subscription
    async fn list_clients(&self, subscription_name: Option<&str>) -> Result<Vec<Client>>;

    /// Update client status (activate/deactivate)
    async fn update_client_status(&self, id: Uuid, active: bool) -> Result<()>;

    /// Delete a client
    async fn delete_client(&self, id: Uuid) -> Result<()>;

    // ===== Event Publishing =====

    /// Publish an event to a topic
    async fn publish_event(&self, event: Event) -> Result<Event>;

    // ===== Event Consumption =====

    /// Poll for events from a subscription (for delivery to clients)
    /// Returns events that need to be delivered
    async fn poll_events(&self, subscription_name: &str, batch_size: usize) -> Result<Vec<Event>>;

    /// Mark an event as delivered successfully
    async fn acknowledge_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()>;

    /// Mark an event delivery as failed
    async fn nack_event(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        error_message: String,
    ) -> Result<()>;

    // ===== Delivery Tracking =====

    /// Record a delivery attempt
    async fn record_delivery_attempt(&self, attempt: DeliveryAttempt) -> Result<()>;

    /// Get delivery attempts for an event
    async fn get_delivery_attempts(&self, event_id: Uuid) -> Result<Vec<DeliveryAttempt>>;

    /// Get pending deliveries (events that need to be delivered to clients)
    async fn get_pending_deliveries(
        &self,
        subscription_name: &str,
        limit: usize,
    ) -> Result<Vec<(Event, Client)>>;

    /// Update delivery status
    async fn update_delivery_status(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        status: DeliveryStatus,
    ) -> Result<()>;

    // ===== Health Check =====

    /// Check if the backend is healthy
    async fn health_check(&self) -> Result<bool>;
}
