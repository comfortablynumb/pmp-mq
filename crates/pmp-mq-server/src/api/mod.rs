mod handlers;
mod routes;

use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub fn create_app(backend: Arc<dyn pmp_mq_core::Backend>) -> Router {
    Router::new()
        .nest("/api/v1", routes::api_routes())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(backend)
}
