use async_trait::async_trait;
use aws_sdk_sqs::Client as SqsClient;
use pmp_mq_core::{
    Backend, Client, CreateSubscriptionRequest, CreateTopicRequest, DeliveryAttempt,
    DeliveryStatus, Event, MqError, RegisterClientRequest, Result, Subscription, Topic,
};
use sqlx::PgPool;
use tracing::debug;
use uuid::Uuid;

use crate::postgres::PostgresBackend;

/// SQS backend uses AWS SQS for event queuing and Postgres for metadata
pub struct SqsBackend {
    sqs_client: SqsClient,
    queue_prefix: String,
    metadata_backend: PostgresBackend,
}

impl SqsBackend {
    pub fn new(sqs_client: SqsClient, queue_prefix: String, metadata_pool: PgPool) -> Self {
        Self {
            sqs_client,
            queue_prefix,
            metadata_backend: PostgresBackend::new(metadata_pool),
        }
    }

    /// Get SQS queue URL for a topic, creating if necessary
    async fn get_or_create_queue_url(&self, topic_name: &str) -> Result<String> {
        let queue_name = format!("{}{}", self.queue_prefix, topic_name);

        // Try to get existing queue URL
        match self
            .sqs_client
            .get_queue_url()
            .queue_name(&queue_name)
            .send()
            .await
        {
            Ok(output) => {
                if let Some(url) = output.queue_url {
                    return Ok(url);
                }
            }
            Err(_) => {
                // Queue doesn't exist, create it
                debug!("Creating SQS queue: {}", queue_name);
            }
        }

        // Create the queue
        let output = self
            .sqs_client
            .create_queue()
            .queue_name(&queue_name)
            .send()
            .await
            .map_err(|e| MqError::BackendError(format!("Failed to create SQS queue: {}", e)))?;

        output
            .queue_url
            .ok_or_else(|| MqError::BackendError("No queue URL returned".to_string()))
    }
}

#[async_trait]
impl Backend for SqsBackend {
    // ===== Topic Management =====

    async fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic> {
        // Create topic in metadata store first
        let topic = self.metadata_backend.create_topic(request).await?;

        // Create corresponding SQS queue
        self.get_or_create_queue_url(&topic.name).await?;

        Ok(topic)
    }

    async fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        self.metadata_backend.get_topic(name).await
    }

    async fn list_topics(&self) -> Result<Vec<Topic>> {
        self.metadata_backend.list_topics().await
    }

    async fn delete_topic(&self, name: &str) -> Result<()> {
        // Delete SQS queue first
        let queue_name = format!("{}{}", self.queue_prefix, name);
        if let Ok(output) = self
            .sqs_client
            .get_queue_url()
            .queue_name(&queue_name)
            .send()
            .await
        {
            if let Some(queue_url) = output.queue_url {
                self.sqs_client
                    .delete_queue()
                    .queue_url(&queue_url)
                    .send()
                    .await
                    .map_err(|e| {
                        MqError::BackendError(format!("Failed to delete SQS queue: {}", e))
                    })?;
            }
        }

        // Then delete from metadata store
        self.metadata_backend.delete_topic(name).await
    }

    // ===== Subscription Management =====

    async fn create_subscription(
        &self,
        request: CreateSubscriptionRequest,
    ) -> Result<Subscription> {
        self.metadata_backend.create_subscription(request).await
    }

    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>> {
        self.metadata_backend.get_subscription(name).await
    }

    async fn list_subscriptions(&self, topic_name: Option<&str>) -> Result<Vec<Subscription>> {
        self.metadata_backend.list_subscriptions(topic_name).await
    }

    async fn delete_subscription(&self, name: &str) -> Result<()> {
        self.metadata_backend.delete_subscription(name).await
    }

    // ===== Client Management =====

    async fn register_client(&self, request: RegisterClientRequest) -> Result<Client> {
        self.metadata_backend.register_client(request).await
    }

    async fn get_client(&self, id: Uuid) -> Result<Option<Client>> {
        self.metadata_backend.get_client(id).await
    }

    async fn list_clients(&self, subscription_name: Option<&str>) -> Result<Vec<Client>> {
        self.metadata_backend.list_clients(subscription_name).await
    }

    async fn update_client_status(&self, id: Uuid, active: bool) -> Result<()> {
        self.metadata_backend.update_client_status(id, active).await
    }

    async fn delete_client(&self, id: Uuid) -> Result<()> {
        self.metadata_backend.delete_client(id).await
    }

    // ===== Event Publishing =====

    async fn publish_event(&self, event: Event) -> Result<Event> {
        // Get queue URL for the topic
        let queue_url = self.get_or_create_queue_url(&event.topic).await?;

        // Serialize event to JSON
        let message_body =
            serde_json::to_string(&event).map_err(|e| MqError::SerializationError(e))?;

        // Send to SQS
        self.sqs_client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&message_body)
            .message_group_id(&event.topic) // For FIFO queues
            .message_deduplication_id(&event.id.to_string()) // For FIFO queues
            .send()
            .await
            .map_err(|e| MqError::BackendError(format!("Failed to send to SQS: {}", e)))?;

        // Also record in metadata store for delivery tracking
        self.metadata_backend.publish_event(event.clone()).await?;

        Ok(event)
    }

    // ===== Event Consumption =====

    async fn poll_events(&self, subscription_name: &str, batch_size: usize) -> Result<Vec<Event>> {
        // For SQS, we could poll from the queue directly, but for webhook delivery
        // we use the metadata store approach for consistency
        self.metadata_backend
            .poll_events(subscription_name, batch_size)
            .await
    }

    async fn acknowledge_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()> {
        self.metadata_backend
            .acknowledge_event(event_id, client_id)
            .await
    }

    async fn nack_event(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        error_message: String,
    ) -> Result<()> {
        self.metadata_backend
            .nack_event(event_id, client_id, error_message)
            .await
    }

    // ===== Delivery Tracking =====

    async fn record_delivery_attempt(&self, attempt: DeliveryAttempt) -> Result<()> {
        self.metadata_backend.record_delivery_attempt(attempt).await
    }

    async fn get_delivery_attempts(&self, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
        self.metadata_backend.get_delivery_attempts(event_id).await
    }

    async fn get_pending_deliveries(
        &self,
        subscription_name: &str,
        limit: usize,
    ) -> Result<Vec<(Event, Client)>> {
        self.metadata_backend
            .get_pending_deliveries(subscription_name, limit)
            .await
    }

    async fn update_delivery_status(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        status: DeliveryStatus,
    ) -> Result<()> {
        self.metadata_backend
            .update_delivery_status(event_id, client_id, status)
            .await
    }

    // ===== Dead Letter Queue Management =====
    // Delegated to metadata backend

    async fn list_dead_letter_events(
        &self,
        subscription_name: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(Event, Client, DeliveryAttempt)>> {
        self.metadata_backend
            .list_dead_letter_events(subscription_name, limit, offset)
            .await
    }

    async fn get_dead_letter_count(&self, subscription_name: Option<&str>) -> Result<i64> {
        self.metadata_backend
            .get_dead_letter_count(subscription_name)
            .await
    }

    async fn retry_dead_letter_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()> {
        self.metadata_backend
            .retry_dead_letter_event(event_id, client_id)
            .await
    }

    async fn delete_dead_letter_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()> {
        self.metadata_backend
            .delete_dead_letter_event(event_id, client_id)
            .await
    }

    async fn bulk_retry_dead_letters(
        &self,
        subscription_name: Option<&str>,
        limit: usize,
    ) -> Result<usize> {
        self.metadata_backend
            .bulk_retry_dead_letters(subscription_name, limit)
            .await
    }

    async fn bulk_delete_dead_letters(
        &self,
        subscription_name: Option<&str>,
        limit: usize,
    ) -> Result<usize> {
        self.metadata_backend
            .bulk_delete_dead_letters(subscription_name, limit)
            .await
    }

    // ===== Health Check =====

    async fn health_check(&self) -> Result<bool> {
        // Check metadata backend
        self.metadata_backend.health_check().await?;

        // Try to list queues to verify SQS connectivity
        self.sqs_client
            .list_queues()
            .send()
            .await
            .map_err(|e| MqError::BackendError(format!("SQS health check failed: {}", e)))?;

        Ok(true)
    }
}
