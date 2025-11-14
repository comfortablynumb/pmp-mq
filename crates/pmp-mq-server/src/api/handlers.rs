use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use pmp_mq_core::{
    Backend, CreateSubscriptionRequest, CreateTopicRequest, Event, MqError, PublishRequest,
    PublishResponse, RegisterClientRequest,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

// ===== Error handling =====

// Wrapper for MqError to implement IntoResponse
pub struct ApiError(MqError);

impl From<MqError> for ApiError {
    fn from(error: MqError) -> Self {
        ApiError(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            MqError::TopicNotFound(msg) => (StatusCode::NOT_FOUND, msg),
            MqError::SubscriptionNotFound(msg) => (StatusCode::NOT_FOUND, msg),
            MqError::ClientNotFound(msg) => (StatusCode::NOT_FOUND, msg),
            MqError::ConfigError(msg) => (StatusCode::BAD_REQUEST, msg),
            MqError::SerializationError(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            MqError::BackendError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            MqError::DatabaseError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            MqError::WebhookError(msg) => (StatusCode::BAD_GATEWAY, msg),
            MqError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

// ===== Health check =====

pub async fn health_check(
    State(backend): State<Arc<dyn Backend>>,
) -> Result<impl IntoResponse, ApiError> {
    backend.health_check().await?;
    Ok(Json(serde_json::json!({ "status": "healthy" })))
}

// ===== Topic handlers =====

pub async fn create_topic(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<CreateTopicRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let topic = backend.create_topic(request).await?;
    Ok((StatusCode::CREATED, Json(topic)))
}

pub async fn get_topic(
    State(backend): State<Arc<dyn Backend>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let topic = backend
        .get_topic(&name)
        .await?
        .ok_or_else(|| MqError::TopicNotFound(name))?;
    Ok(Json(topic))
}

pub async fn list_topics(
    State(backend): State<Arc<dyn Backend>>,
) -> Result<impl IntoResponse, ApiError> {
    let topics = backend.list_topics().await?;
    Ok(Json(topics))
}

pub async fn delete_topic(
    State(backend): State<Arc<dyn Backend>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    backend.delete_topic(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ===== Subscription handlers =====

pub async fn create_subscription(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<CreateSubscriptionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let subscription = backend.create_subscription(request).await?;
    Ok((StatusCode::CREATED, Json(subscription)))
}

pub async fn get_subscription(
    State(backend): State<Arc<dyn Backend>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let subscription = backend
        .get_subscription(&name)
        .await?
        .ok_or_else(|| MqError::SubscriptionNotFound(name))?;
    Ok(Json(subscription))
}

#[derive(Deserialize)]
pub struct ListSubscriptionsQuery {
    topic: Option<String>,
}

pub async fn list_subscriptions(
    State(backend): State<Arc<dyn Backend>>,
    Query(query): Query<ListSubscriptionsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let subscriptions = backend
        .list_subscriptions(query.topic.as_deref())
        .await?;
    Ok(Json(subscriptions))
}

pub async fn delete_subscription(
    State(backend): State<Arc<dyn Backend>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    backend.delete_subscription(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ===== Client handlers =====

pub async fn register_client(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<RegisterClientRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let client = backend.register_client(request).await?;
    Ok((StatusCode::CREATED, Json(client)))
}

pub async fn get_client(
    State(backend): State<Arc<dyn Backend>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let client = backend
        .get_client(id)
        .await?
        .ok_or_else(|| MqError::ClientNotFound(id.to_string()))?;
    Ok(Json(client))
}

#[derive(Deserialize)]
pub struct ListClientsQuery {
    subscription: Option<String>,
}

pub async fn list_clients(
    State(backend): State<Arc<dyn Backend>>,
    Query(query): Query<ListClientsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let clients = backend
        .list_clients(query.subscription.as_deref())
        .await?;
    Ok(Json(clients))
}

pub async fn delete_client(
    State(backend): State<Arc<dyn Backend>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    backend.delete_client(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct UpdateClientStatusRequest {
    active: bool,
}

pub async fn update_client_status(
    State(backend): State<Arc<dyn Backend>>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateClientStatusRequest>,
) -> Result<impl IntoResponse, ApiError> {
    backend.update_client_status(id, request.active).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ===== Event handlers =====

pub async fn publish_event(
    State(backend): State<Arc<dyn Backend>>,
    Json(request): Json<PublishRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut event = Event::new(request.topic, request.payload);
    if let Some(event_type) = request.event_type {
        event = event.with_event_type(event_type);
    }
    event = event.with_metadata(request.metadata);

    let published_event = backend.publish_event(event).await?;

    let response = PublishResponse {
        event_id: published_event.id,
        topic: published_event.topic,
        published_at: published_event.created_at,
    };

    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn get_delivery_attempts(
    State(backend): State<Arc<dyn Backend>>,
    Path(event_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let attempts = backend.get_delivery_attempts(event_id).await?;
    Ok(Json(attempts))
}
