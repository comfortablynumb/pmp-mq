use chrono::Utc;
use pmp_mq_core::{Backend, DeliveryAttempt, DeliveryStatus, FilterExpression};
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::DeliveryConfig;

/// Run the webhook delivery worker
pub async fn run_delivery_worker(backend: Arc<dyn Backend>, config: DeliveryConfig) {
    info!("Starting delivery worker");

    let http_client = Client::builder()
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .expect("Failed to create HTTP client");

    loop {
        if let Err(e) = process_deliveries(&backend, &http_client, &config).await {
            error!("Error processing deliveries: {}", e);
        }

        sleep(Duration::from_secs(config.worker_interval_secs)).await;
    }
}

async fn process_deliveries(
    backend: &Arc<dyn Backend>,
    http_client: &Client,
    config: &DeliveryConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get all subscriptions
    let subscriptions = backend.list_subscriptions(None).await?;

    for subscription in subscriptions {
        debug!("Processing subscription: {}", subscription.name);

        // Parse subscription filter
        let filter = if let Some(ref filter_str) = subscription.filter {
            match FilterExpression::parse(filter_str) {
                Ok(f) => Some(f),
                Err(e) => {
                    warn!(
                        "Invalid filter for subscription {}: {}",
                        subscription.name, e
                    );
                    None
                }
            }
        } else {
            None
        };

        // Get pending deliveries for this subscription
        let pending = backend
            .get_pending_deliveries(&subscription.name, config.batch_size)
            .await?;

        if pending.is_empty() {
            continue;
        }

        // Filter events if filter is defined
        let filtered_pending: Vec<_> = if let Some(ref filter_expr) = filter {
            pending
                .into_iter()
                .filter(|(event, _)| {
                    let matches = filter_expr.matches(event);
                    if !matches {
                        debug!("Event {} filtered out by subscription filter", event.id);
                    }
                    matches
                })
                .collect()
        } else {
            pending
        };

        if filtered_pending.is_empty() {
            continue;
        }

        info!(
            "Found {} pending deliveries for subscription {} (after filtering)",
            filtered_pending.len(),
            subscription.name
        );

        // Process deliveries concurrently with a limit
        let mut tasks = Vec::new();

        for (event, client) in filtered_pending {
            if tasks.len() >= config.max_concurrent_deliveries {
                // Wait for some tasks to complete
                let (result, _index, remaining) = futures::future::select_all(tasks).await;
                tasks = remaining;

                if let Err(e) = result {
                    error!("Delivery task failed: {}", e);
                }
            }

            let backend = Arc::clone(backend);
            let http_client = http_client.clone();
            let event = event.clone();
            let client = client.clone();

            let task = tokio::spawn(async move {
                deliver_event(&backend, &http_client, &event, &client).await;
            });

            tasks.push(task);
        }

        // Wait for remaining tasks to complete
        for task in tasks {
            if let Err(e) = task.await {
                error!("Delivery task failed: {}", e);
            }
        }
    }

    Ok(())
}

async fn deliver_event(
    backend: &Arc<dyn Backend>,
    http_client: &Client,
    event: &pmp_mq_core::Event,
    client: &pmp_mq_core::Client,
) {
    debug!(
        "Delivering event {} to client {} at {}",
        event.id, client.id, client.webhook_url
    );

    // Mark delivery as in progress
    if let Err(e) = backend
        .update_delivery_status(event.id, client.id, DeliveryStatus::InProgress)
        .await
    {
        error!("Failed to update delivery status: {}", e);
        return;
    }

    // Get current attempt number
    let attempts = match backend.get_delivery_attempts(event.id).await {
        Ok(attempts) => attempts.iter().filter(|a| a.client_id == client.id).count() as i32,
        Err(e) => {
            error!("Failed to get delivery attempts: {}", e);
            0
        }
    };

    let attempt_number = attempts + 1;
    let attempt_id = Uuid::new_v4();
    let attempted_at = Utc::now();

    // Build HTTP request
    let mut request = http_client.post(&client.webhook_url).json(&event);

    // Add auth headers if present
    for (key, value) in &client.auth_headers {
        request = request.header(key, value);
    }

    // Send request
    let result = request.send().await;

    match result {
        Ok(response) => {
            let status_code = response.status().as_u16();
            let success = response.status().is_success();

            debug!(
                "Webhook response: status={}, success={}",
                status_code, success
            );

            let attempt = DeliveryAttempt {
                id: attempt_id,
                event_id: event.id,
                client_id: client.id,
                attempt_number,
                status: if success {
                    DeliveryStatus::Success
                } else {
                    DeliveryStatus::Failed
                },
                attempted_at,
                completed_at: Some(Utc::now()),
                error_message: if success {
                    None
                } else {
                    Some(format!("HTTP status: {}", status_code))
                },
                http_status_code: Some(status_code),
            };

            // Record attempt
            if let Err(e) = backend.record_delivery_attempt(attempt).await {
                error!("Failed to record delivery attempt: {}", e);
            }

            // Update delivery status
            if success {
                info!(
                    "Successfully delivered event {} to client {}",
                    event.id, client.id
                );
                if let Err(e) = backend.acknowledge_event(event.id, client.id).await {
                    error!("Failed to acknowledge event: {}", e);
                }
            } else {
                warn!(
                    "Failed to deliver event {} to client {}: HTTP {}",
                    event.id, client.id, status_code
                );
                if let Err(e) = backend
                    .nack_event(event.id, client.id, format!("HTTP status: {}", status_code))
                    .await
                {
                    error!("Failed to nack event: {}", e);
                }
            }
        }
        Err(e) => {
            error!(
                "Failed to deliver event {} to client {}: {}",
                event.id, client.id, e
            );

            let attempt = DeliveryAttempt {
                id: attempt_id,
                event_id: event.id,
                client_id: client.id,
                attempt_number,
                status: DeliveryStatus::Failed,
                attempted_at,
                completed_at: Some(Utc::now()),
                error_message: Some(e.to_string()),
                http_status_code: None,
            };

            // Record attempt
            if let Err(e) = backend.record_delivery_attempt(attempt).await {
                error!("Failed to record delivery attempt: {}", e);
            }

            // Update delivery status
            if let Err(e) = backend.nack_event(event.id, client.id, e.to_string()).await {
                error!("Failed to nack event: {}", e);
            }
        }
    }
}
