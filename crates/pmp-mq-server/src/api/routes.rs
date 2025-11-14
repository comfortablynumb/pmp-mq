use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use super::handlers;

pub fn api_routes() -> Router<Arc<dyn pmp_mq_core::Backend>> {
    Router::new()
        // Health check
        .route("/health", get(handlers::health_check))
        // Topics
        .route("/topics", post(handlers::create_topic))
        .route("/topics", get(handlers::list_topics))
        .route("/topics/:name", get(handlers::get_topic))
        .route("/topics/:name", delete(handlers::delete_topic))
        // Subscriptions
        .route("/subscriptions", post(handlers::create_subscription))
        .route("/subscriptions", get(handlers::list_subscriptions))
        .route("/subscriptions/:name", get(handlers::get_subscription))
        .route("/subscriptions/:name", delete(handlers::delete_subscription))
        // Clients
        .route("/clients", post(handlers::register_client))
        .route("/clients", get(handlers::list_clients))
        .route("/clients/:id", get(handlers::get_client))
        .route("/clients/:id", delete(handlers::delete_client))
        .route(
            "/clients/:id/status",
            put(handlers::update_client_status),
        )
        // Events
        .route("/events/publish", post(handlers::publish_event))
        .route("/events/:id/attempts", get(handlers::get_delivery_attempts))
}
