use pmp_mq_backends::postgres::PostgresBackend;
/**
 * Integration tests for PMP-MQ
 *
 * These tests cover end-to-end functionality including:
 * - Event publishing and delivery
 * - Event filtering
 * - Batch operations
 * - Dead letter queue management
 * - Retry logic
 */
use pmp_mq_core::{
    Backend, CreateSubscriptionRequest, CreateTopicRequest, Event, FilterExpression,
    RegisterClientRequest, SubscriptionConfig, TopicConfig,
};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

// Helper function to create test backend
async fn create_test_backend() -> Arc<PostgresBackend> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://pmp_user:pmp_password@localhost:5432/pmp_mq".to_string());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");

    // Run migrations
    pmp_mq_backends::postgres::migrations::run_migrations(&pool)
        .await
        .expect("Failed to run migrations");

    Arc::new(PostgresBackend::new(pool))
}

// Helper function to cleanup test data
async fn cleanup_test_data(backend: &Arc<PostgresBackend>, topic_name: &str) {
    let _ = backend.delete_topic(topic_name).await;
}

#[tokio::test]
async fn test_basic_event_publishing() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());

    // Create topic
    let topic = backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: Some("Test topic".to_string()),
            config: TopicConfig {
                retention_seconds: None,
                max_message_size: None,
                store_events: true,
            },
        })
        .await
        .expect("Failed to create topic");

    assert_eq!(topic.name, topic_name);

    // Publish event
    let event = Event::new(topic_name.clone(), serde_json::json!({"test": "data"}));
    let published = backend
        .publish_event(event)
        .await
        .expect("Failed to publish event");

    assert_eq!(published.topic, topic_name);

    // Cleanup
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_subscription_and_client() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let subscription_name = format!("test-sub-{}", Uuid::new_v4());

    // Create topic
    backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: None,
            config: TopicConfig::default(),
        })
        .await
        .expect("Failed to create topic");

    // Create subscription
    let subscription = backend
        .create_subscription(CreateSubscriptionRequest {
            name: subscription_name.clone(),
            topic_name: topic_name.clone(),
            filter: None,
            config: SubscriptionConfig::default(),
        })
        .await
        .expect("Failed to create subscription");

    assert_eq!(subscription.name, subscription_name);
    assert_eq!(subscription.topic_name, topic_name);

    // Register client
    let client = backend
        .register_client(RegisterClientRequest {
            name: "test-client".to_string(),
            webhook_url: "http://localhost:8000/webhook".to_string(),
            subscription_name: subscription_name.clone(),
            auth_headers: Default::default(),
        })
        .await
        .expect("Failed to register client");

    assert_eq!(client.subscription_name, subscription_name);
    assert!(client.active);

    // List clients
    let clients = backend
        .list_clients(Some(&subscription_name))
        .await
        .expect("Failed to list clients");

    assert_eq!(clients.len(), 1);

    // Cleanup
    backend.delete_client(client.id).await.ok();
    backend.delete_subscription(&subscription_name).await.ok();
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_event_filtering() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());

    // Create topic
    backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: None,
            config: TopicConfig {
                store_events: true,
                ..Default::default()
            },
        })
        .await
        .expect("Failed to create topic");

    // Test event type filter
    let filter =
        FilterExpression::parse("event_type == 'user.created'").expect("Failed to parse filter");

    let matching_event = Event::new(topic_name.clone(), serde_json::json!({"user_id": "123"}))
        .with_event_type("user.created".to_string());

    let non_matching_event = Event::new(topic_name.clone(), serde_json::json!({"order_id": "456"}))
        .with_event_type("order.placed".to_string());

    assert!(filter.matches(&matching_event));
    assert!(!filter.matches(&non_matching_event));

    // Test JSONPath filter
    let json_filter =
        FilterExpression::parse("$.action == 'signup'").expect("Failed to parse JSONPath filter");

    let signup_event = Event::new(
        topic_name.clone(),
        serde_json::json!({"action": "signup", "user_id": "789"}),
    );

    let login_event = Event::new(
        topic_name.clone(),
        serde_json::json!({"action": "login", "user_id": "789"}),
    );

    assert!(json_filter.matches(&signup_event));
    assert!(!json_filter.matches(&login_event));

    // Cleanup
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_event_delivery_workflow() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let subscription_name = format!("test-sub-{}", Uuid::new_v4());

    // Create topic with event storage
    backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: None,
            config: TopicConfig {
                store_events: true,
                ..Default::default()
            },
        })
        .await
        .expect("Failed to create topic");

    // Create subscription
    backend
        .create_subscription(CreateSubscriptionRequest {
            name: subscription_name.clone(),
            topic_name: topic_name.clone(),
            filter: None,
            config: SubscriptionConfig::default(),
        })
        .await
        .expect("Failed to create subscription");

    // Register client
    let client = backend
        .register_client(RegisterClientRequest {
            name: "test-client".to_string(),
            webhook_url: "http://localhost:9999/webhook".to_string(),
            subscription_name: subscription_name.clone(),
            auth_headers: Default::default(),
        })
        .await
        .expect("Failed to register client");

    // Publish event
    let event = Event::new(topic_name.clone(), serde_json::json!({"test": "delivery"}));
    let published = backend
        .publish_event(event)
        .await
        .expect("Failed to publish event");

    // Wait a bit for delivery record to be created
    sleep(Duration::from_millis(100)).await;

    // Check pending deliveries
    let pending = backend
        .get_pending_deliveries(&subscription_name, 10)
        .await
        .expect("Failed to get pending deliveries");

    assert!(
        !pending.is_empty(),
        "Should have at least one pending delivery"
    );
    assert_eq!(pending[0].0.id, published.id);
    assert_eq!(pending[0].1.id, client.id);

    // Cleanup
    backend.delete_client(client.id).await.ok();
    backend.delete_subscription(&subscription_name).await.ok();
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_dead_letter_queue() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());
    let subscription_name = format!("test-sub-{}", Uuid::new_v4());

    // Create topic
    backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: None,
            config: TopicConfig {
                store_events: true,
                ..Default::default()
            },
        })
        .await
        .expect("Failed to create topic");

    // Create subscription
    backend
        .create_subscription(CreateSubscriptionRequest {
            name: subscription_name.clone(),
            topic_name: topic_name.clone(),
            filter: None,
            config: SubscriptionConfig::default(),
        })
        .await
        .expect("Failed to create subscription");

    // Register client
    let client = backend
        .register_client(RegisterClientRequest {
            name: "test-client".to_string(),
            webhook_url: "http://localhost:9999/webhook".to_string(),
            subscription_name: subscription_name.clone(),
            auth_headers: Default::default(),
        })
        .await
        .expect("Failed to register client");

    // Publish event
    let event = Event::new(topic_name.clone(), serde_json::json!({"test": "dlq"}));
    let published = backend
        .publish_event(event)
        .await
        .expect("Failed to publish event");

    // Wait for delivery record
    sleep(Duration::from_millis(100)).await;

    // Manually mark as dead letter (simulating failed retries)
    use pmp_mq_core::DeliveryStatus;
    backend
        .update_delivery_status(published.id, client.id, DeliveryStatus::DeadLetter)
        .await
        .expect("Failed to update delivery status");

    // List DLQ events
    let dlq_events = backend
        .list_dead_letter_events(Some(&subscription_name), 10, 0)
        .await
        .expect("Failed to list DLQ events");

    assert_eq!(dlq_events.len(), 1);
    assert_eq!(dlq_events[0].0.id, published.id);

    // Test DLQ count
    let dlq_count = backend
        .get_dead_letter_count(Some(&subscription_name))
        .await
        .expect("Failed to get DLQ count");

    assert_eq!(dlq_count, 1);

    // Test retry from DLQ
    backend
        .retry_dead_letter_event(published.id, client.id)
        .await
        .expect("Failed to retry DLQ event");

    // Verify it's back in pending
    let dlq_count_after_retry = backend
        .get_dead_letter_count(Some(&subscription_name))
        .await
        .expect("Failed to get DLQ count after retry");

    assert_eq!(dlq_count_after_retry, 0);

    // Cleanup
    backend.delete_client(client.id).await.ok();
    backend.delete_subscription(&subscription_name).await.ok();
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_topic_without_event_storage() {
    let backend = create_test_backend().await;
    let topic_name = format!("test-topic-{}", Uuid::new_v4());

    // Create topic without event storage
    backend
        .create_topic(CreateTopicRequest {
            name: topic_name.clone(),
            description: None,
            config: TopicConfig {
                store_events: false, // Don't store events
                ..Default::default()
            },
        })
        .await
        .expect("Failed to create topic");

    // Publish event
    let event = Event::new(
        topic_name.clone(),
        serde_json::json!({"test": "no-storage"}),
    );
    let published = backend
        .publish_event(event)
        .await
        .expect("Failed to publish event");

    assert_eq!(published.topic, topic_name);

    // Cleanup
    cleanup_test_data(&backend, &topic_name).await;
}

#[tokio::test]
async fn test_health_check() {
    let backend = create_test_backend().await;

    let healthy = backend.health_check().await.expect("Health check failed");

    assert!(healthy);
}
