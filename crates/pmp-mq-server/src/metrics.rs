use lazy_static::lazy_static;
use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::Arc;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Event metrics
    pub static ref EVENTS_PUBLISHED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pmp_mq_events_published_total", "Total number of events published"),
        &["topic", "backend"]
    ).unwrap();

    pub static ref EVENTS_DELIVERED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pmp_mq_events_delivered_total", "Total number of events delivered"),
        &["topic", "subscription", "status"]
    ).unwrap();

    pub static ref EVENTS_FILTERED_TOTAL: IntCounter = IntCounter::new(
        "pmp_mq_events_filtered_total",
        "Total number of events filtered out"
    ).unwrap();

    // Delivery metrics
    pub static ref DELIVERY_ATTEMPTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pmp_mq_delivery_attempts_total", "Total number of delivery attempts"),
        &["subscription", "client"]
    ).unwrap();

    pub static ref DELIVERY_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("pmp_mq_delivery_latency_seconds", "Delivery latency in seconds")
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    pub static ref DELIVERY_FAILURES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pmp_mq_delivery_failures_total", "Total number of failed deliveries"),
        &["subscription", "error_type"]
    ).unwrap();

    // Queue metrics
    pub static ref PENDING_DELIVERIES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("pmp_mq_pending_deliveries", "Number of pending deliveries"),
        &["subscription"]
    ).unwrap();

    pub static ref DEAD_LETTER_QUEUE_SIZE: IntGauge = IntGauge::new(
        "pmp_mq_dead_letter_queue_size",
        "Number of events in dead letter queue"
    ).unwrap();

    // Resource metrics
    pub static ref TOPICS_TOTAL: IntGauge = IntGauge::new(
        "pmp_mq_topics_total",
        "Total number of topics"
    ).unwrap();

    pub static ref SUBSCRIPTIONS_TOTAL: IntGauge = IntGauge::new(
        "pmp_mq_subscriptions_total",
        "Total number of subscriptions"
    ).unwrap();

    pub static ref CLIENTS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("pmp_mq_clients_total", "Total number of clients"),
        &["active"]
    ).unwrap();

    // HTTP metrics
    pub static ref HTTP_REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pmp_mq_http_requests_total", "Total number of HTTP requests"),
        &["method", "path", "status"]
    ).unwrap();

    pub static ref HTTP_REQUEST_DURATION_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("pmp_mq_http_request_duration_seconds", "HTTP request duration in seconds")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
    ).unwrap();
}

/// Initialize metrics registry
pub fn init_metrics() {
    // Register all metrics
    REGISTRY
        .register(Box::new(EVENTS_PUBLISHED_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(EVENTS_DELIVERED_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(EVENTS_FILTERED_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DELIVERY_ATTEMPTS_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DELIVERY_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DELIVERY_FAILURES_TOTAL.clone()))
        .ok();
    REGISTRY.register(Box::new(PENDING_DELIVERIES.clone())).ok();
    REGISTRY
        .register(Box::new(DEAD_LETTER_QUEUE_SIZE.clone()))
        .ok();
    REGISTRY.register(Box::new(TOPICS_TOTAL.clone())).ok();
    REGISTRY
        .register(Box::new(SUBSCRIPTIONS_TOTAL.clone()))
        .ok();
    REGISTRY.register(Box::new(CLIENTS_TOTAL.clone())).ok();
    REGISTRY
        .register(Box::new(HTTP_REQUESTS_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(HTTP_REQUEST_DURATION_SECONDS.clone()))
        .ok();
}

/// Get Prometheus-formatted metrics
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Update resource metrics from backend
pub async fn update_resource_metrics(backend: &Arc<dyn pmp_mq_core::Backend>) {
    if let Ok(topics) = backend.list_topics().await {
        TOPICS_TOTAL.set(topics.len() as i64);
    }

    if let Ok(subscriptions) = backend.list_subscriptions(None).await {
        SUBSCRIPTIONS_TOTAL.set(subscriptions.len() as i64);
    }

    if let Ok(clients) = backend.list_clients(None).await {
        let active = clients.iter().filter(|c| c.active).count();
        let inactive = clients.len() - active;
        CLIENTS_TOTAL
            .with_label_values(&["true"])
            .set(active as i64);
        CLIENTS_TOTAL
            .with_label_values(&["false"])
            .set(inactive as i64);
    }
}
