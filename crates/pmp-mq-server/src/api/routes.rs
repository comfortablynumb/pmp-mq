use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use super::{handlers, handlers_ext};

pub fn api_routes() -> Router<Arc<dyn pmp_mq_core::Backend>> {
    Router::new()
        // Health check
        .route("/health", get(handlers::health_check))
        // Metrics
        .route("/metrics", get(handlers_ext::get_system_metrics))
        .route("/metrics/prometheus", get(handlers_ext::prometheus_metrics))
        // Topics
        .route("/topics", post(handlers::create_topic))
        .route("/topics", get(handlers::list_topics))
        .route("/topics/:name", get(handlers::get_topic))
        .route("/topics/:name", delete(handlers::delete_topic))
        // Subscriptions
        .route("/subscriptions", post(handlers::create_subscription))
        .route("/subscriptions", get(handlers::list_subscriptions))
        .route("/subscriptions/:name", get(handlers::get_subscription))
        .route(
            "/subscriptions/:name",
            delete(handlers::delete_subscription),
        )
        // Clients
        .route("/clients", post(handlers::register_client))
        .route("/clients", get(handlers::list_clients))
        .route("/clients/:id", get(handlers::get_client))
        .route("/clients/:id", delete(handlers::delete_client))
        .route("/clients/:id/status", put(handlers::update_client_status))
        // Events
        .route("/events/publish", post(handlers::publish_event))
        .route("/events/batch", post(handlers_ext::batch_publish_events))
        .route("/events/:id/attempts", get(handlers::get_delivery_attempts))
        // Dead Letter Queue
        .route("/dlq/events", get(handlers_ext::list_dead_letter_events))
        .route(
            "/dlq/events/:event_id/:client_id/retry",
            post(handlers_ext::retry_dead_letter_event),
        )
        .route(
            "/dlq/events/:event_id/:client_id",
            delete(handlers_ext::delete_dead_letter_event),
        )
        .route(
            "/dlq/bulk-retry",
            post(handlers_ext::bulk_retry_dead_letters),
        )
        .route(
            "/dlq/bulk-delete",
            post(handlers_ext::bulk_delete_dead_letters),
        )
}
