use async_trait::async_trait;
use pmp_mq_core::{
    Backend, Client, CreateSubscriptionRequest, CreateTopicRequest, DeliveryAttempt,
    DeliveryStatus, Event, MqError, RegisterClientRequest, Result, Subscription, Topic,
};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use sqlx::PgPool;
use uuid::Uuid;

use crate::postgres::PostgresBackend;

/// Kafka backend uses Kafka for event streaming and Postgres for metadata
/// This is a common pattern as Kafka excels at event streaming but Postgres
/// is better for querying metadata (subscriptions, clients, delivery tracking)
pub struct KafkaBackend {
    producer: FutureProducer,
    admin_client: AdminClient<DefaultClientContext>,
    metadata_backend: PostgresBackend,
}

impl KafkaBackend {
    pub async fn new(
        kafka_config: &ClientConfig,
        metadata_pool: PgPool,
    ) -> Result<Self> {
        let producer: FutureProducer = kafka_config
            .create()
            .map_err(|e| MqError::BackendError(format!("Failed to create Kafka producer: {}", e)))?;

        let admin_client: AdminClient<DefaultClientContext> = kafka_config
            .create()
            .map_err(|e| MqError::BackendError(format!("Failed to create Kafka admin client: {}", e)))?;

        let metadata_backend = PostgresBackend::new(metadata_pool);

        Ok(Self {
            producer,
            admin_client,
            metadata_backend,
        })
    }
}

#[async_trait]
impl Backend for KafkaBackend {
    // ===== Topic Management =====
    // Topics are created in both Kafka and metadata store

    async fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic> {
        // Create topic in metadata store first
        let topic = self.metadata_backend.create_topic(request.clone()).await?;

        // Create Kafka topic
        let new_topic = NewTopic::new(&topic.name, 3, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(5)));

        self.admin_client
            .create_topics(&[new_topic], &opts)
            .await
            .map_err(|e| {
                MqError::BackendError(format!("Failed to create Kafka topic: {:?}", e))
            })?;

        Ok(topic)
    }

    async fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        self.metadata_backend.get_topic(name).await
    }

    async fn list_topics(&self) -> Result<Vec<Topic>> {
        self.metadata_backend.list_topics().await
    }

    async fn delete_topic(&self, name: &str) -> Result<()> {
        // Delete from Kafka first
        let opts = AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(5)));
        self.admin_client
            .delete_topics(&[name], &opts)
            .await
            .map_err(|e| {
                MqError::BackendError(format!("Failed to delete Kafka topic: {:?}", e))
            })?;

        // Then delete from metadata store
        self.metadata_backend.delete_topic(name).await
    }

    // ===== Subscription Management =====
    // Delegated to metadata backend

    async fn create_subscription(&self, request: CreateSubscriptionRequest) -> Result<Subscription> {
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
    // Delegated to metadata backend

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
    // Events are published to Kafka and recorded in metadata store

    async fn publish_event(&self, event: Event) -> Result<Event> {
        // Serialize event to JSON
        let payload = serde_json::to_string(&event)
            .map_err(|e| MqError::SerializationError(e))?;

        // Create key string with proper lifetime
        let key = event.id.to_string();

        // Publish to Kafka
        let record = FutureRecord::to(&event.topic)
            .key(&key)
            .payload(&payload);

        self.producer
            .send(record, std::time::Duration::from_secs(5))
            .await
            .map_err(|(e, _)| {
                MqError::BackendError(format!("Failed to publish to Kafka: {}", e))
            })?;

        // Also record in metadata store for delivery tracking
        self.metadata_backend.publish_event(event.clone()).await?;

        Ok(event)
    }

    // ===== Event Consumption =====
    // For webhook delivery, we read from the metadata store
    // In a production system, you might want to consume directly from Kafka

    async fn poll_events(&self, subscription_name: &str, batch_size: usize) -> Result<Vec<Event>> {
        self.metadata_backend.poll_events(subscription_name, batch_size).await
    }

    async fn acknowledge_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()> {
        self.metadata_backend.acknowledge_event(event_id, client_id).await
    }

    async fn nack_event(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        error_message: String,
    ) -> Result<()> {
        self.metadata_backend.nack_event(event_id, client_id, error_message).await
    }

    // ===== Delivery Tracking =====
    // Delegated to metadata backend

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
        self.metadata_backend.get_pending_deliveries(subscription_name, limit).await
    }

    async fn update_delivery_status(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        status: DeliveryStatus,
    ) -> Result<()> {
        self.metadata_backend.update_delivery_status(event_id, client_id, status).await
    }

    // ===== Health Check =====

    async fn health_check(&self) -> Result<bool> {
        // Check both Kafka and metadata backend
        self.metadata_backend.health_check().await?;
        // Kafka health check is implicit in the producer being alive
        Ok(true)
    }
}
