use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use pmp_mq_core::{
    Backend, BatchPublishError, BatchPublishRequest, BatchPublishResponse, Event,
    PublishResponse, SystemMetrics,
};
use std::sync::Arc;

use super::handlers::ApiError;

// ===== Batch Publishing =====

pub async fn batch_publish_events(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<BatchPublishRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut published = Vec::new();
    let mut failed = Vec::new();
    let total = request.events.len();

    for (index, event_req) in request.events.into_iter().enumerate() {
        let mut event = Event::new(event_req.topic.clone(), event_req.payload);
        if let Some(event_type) = event_req.event_type {
            event = event.with_event_type(event_type);
        }
        event = event.with_metadata(event_req.metadata);

        match backend.publish_event(event).await {
            Ok(published_event) => {
                published.push(PublishResponse {
                    event_id: published_event.id,
                    topic: published_event.topic,
                    published_at: published_event.created_at,
                });
            }
            Err(e) => {
                failed.push(BatchPublishError {
                    index,
                    topic: event_req.topic,
                    error: e.to_string(),
                });
            }
        }
    }

    let response = BatchPublishResponse {
        success_count: published.len(),
        failure_count: failed.len(),
        total,
        published,
        failed,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

// ===== Metrics =====

pub async fn get_system_metrics(
    State(backend): State<Arc<dyn Backend>>,
) -> Result<impl IntoResponse, ApiError> {
    // Get counts from backend
    let topics = backend.list_topics().await?;
    let subscriptions = backend.list_subscriptions(None).await?;
    let clients = backend.list_clients(None).await?;

    let active_clients = clients.iter().filter(|c| c.active).count();

    // Note: This is a simplified implementation
    // In production, you'd want to add these as dedicated backend methods
    // for better performance
    let metrics = SystemMetrics {
        topics_count: topics.len() as i64,
        subscriptions_count: subscriptions.len() as i64,
        clients_count: clients.len() as i64,
        active_clients_count: active_clients as i64,
        pending_deliveries: 0, // Would need dedicated query
        failed_deliveries: 0,  // Would need dedicated query
        dead_letter_count: 0,  // Would need dedicated query
    };

    Ok(Json(metrics))
}
