use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use pmp_mq_core::{
    Backend, BatchPublishError, BatchPublishRequest, BatchPublishResponse, BulkDeadLetterResponse,
    DeadLetterEvent, Event, ListDeadLetterResponse, PublishResponse, SystemMetrics,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use super::handlers::ApiError;
use crate::metrics;

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

// ===== Prometheus Metrics =====

pub async fn prometheus_metrics(State(backend): State<Arc<dyn Backend>>) -> impl IntoResponse {
    // Update resource metrics before gathering
    metrics::update_resource_metrics(&backend).await;

    // Return Prometheus-formatted metrics
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::gather_metrics(),
    )
}

// ===== Dead Letter Queue Management =====

#[derive(Debug, Deserialize)]
pub struct ListDlqQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    pub subscription_name: Option<String>,
}

fn default_limit() -> usize {
    50
}

#[derive(Debug, Deserialize)]
pub struct BulkDlqRequest {
    #[serde(default = "default_bulk_limit")]
    pub limit: usize,
    pub subscription_name: Option<String>,
}

fn default_bulk_limit() -> usize {
    100
}

/// List dead letter queue events
pub async fn list_dead_letter_events(
    State(backend): State<Arc<dyn Backend>>,
    Query(query): Query<ListDlqQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let subscription_name = query.subscription_name.as_deref();

    // Get total count
    let total = backend.get_dead_letter_count(subscription_name).await?;

    // Get events
    let dlq_events = backend
        .list_dead_letter_events(subscription_name, query.limit, query.offset)
        .await?;

    let events: Vec<DeadLetterEvent> = dlq_events
        .into_iter()
        .map(|(event, client, last_attempt)| DeadLetterEvent {
            total_attempts: last_attempt.attempt_number,
            event,
            client,
            last_attempt,
        })
        .collect();

    let response = ListDeadLetterResponse {
        total,
        events,
        offset: query.offset,
        limit: query.limit,
    };

    Ok(Json(response))
}

/// Retry a specific dead letter event
pub async fn retry_dead_letter_event(
    State(backend): State<Arc<dyn Backend>>,
    Path((event_id, client_id)): Path<(Uuid, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    backend.retry_dead_letter_event(event_id, client_id).await?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Event moved back to pending queue"
        })),
    ))
}

/// Delete a specific dead letter event
pub async fn delete_dead_letter_event(
    State(backend): State<Arc<dyn Backend>>,
    Path((event_id, client_id)): Path<(Uuid, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    backend
        .delete_dead_letter_event(event_id, client_id)
        .await?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "success",
            "message": "Dead letter event deleted"
        })),
    ))
}

/// Bulk retry dead letter events
pub async fn bulk_retry_dead_letters(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<BulkDlqRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let affected = backend
        .bulk_retry_dead_letters(request.subscription_name.as_deref(), request.limit)
        .await?;

    Ok(Json(BulkDeadLetterResponse {
        affected_count: affected,
    }))
}

/// Bulk delete dead letter events
pub async fn bulk_delete_dead_letters(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<BulkDlqRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let affected = backend
        .bulk_delete_dead_letters(request.subscription_name.as_deref(), request.limit)
        .await?;

    Ok(Json(BulkDeadLetterResponse {
        affected_count: affected,
    }))
}
