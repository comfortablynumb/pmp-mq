use async_trait::async_trait;
use chrono::Utc;
use pmp_mq_core::{
    Backend, Client, CreateSubscriptionRequest, CreateTopicRequest, DeliveryAttempt,
    DeliveryStatus, Event, MqError, RegisterClientRequest, Result, Subscription, Topic,
};
use sqlx::PgPool;
use uuid::Uuid;

pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    // ===== Topic Management =====

    async fn create_topic(&self, request: CreateTopicRequest) -> Result<Topic> {
        let topic = Topic {
            id: Uuid::new_v4(),
            name: request.name,
            description: request.description,
            created_at: Utc::now(),
            config: request.config,
        };

        sqlx::query(
            r#"
            INSERT INTO topics (id, name, description, created_at, config)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&topic.id)
        .bind(&topic.name)
        .bind(&topic.description)
        .bind(&topic.created_at)
        .bind(serde_json::to_value(&topic.config)?)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to create topic: {}", e)))?;

        Ok(topic)
    }

    async fn get_topic(&self, name: &str) -> Result<Option<Topic>> {
        let topic = sqlx::query_as::<_, (Uuid, String, Option<String>, chrono::DateTime<Utc>, serde_json::Value)>(
            "SELECT id, name, description, created_at, config FROM topics WHERE name = $1"
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get topic: {}", e)))?
        .map(|(id, name, description, created_at, config)| Topic {
            id,
            name,
            description,
            created_at,
            config: serde_json::from_value(config).unwrap_or_default(),
        });

        Ok(topic)
    }

    async fn list_topics(&self) -> Result<Vec<Topic>> {
        let topics = sqlx::query_as::<_, (Uuid, String, Option<String>, chrono::DateTime<Utc>, serde_json::Value)>(
            "SELECT id, name, description, created_at, config FROM topics ORDER BY created_at DESC"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to list topics: {}", e)))?
        .into_iter()
        .map(|(id, name, description, created_at, config)| Topic {
            id,
            name,
            description,
            created_at,
            config: serde_json::from_value(config).unwrap_or_default(),
        })
        .collect();

        Ok(topics)
    }

    async fn delete_topic(&self, name: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM topics WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| MqError::DatabaseError(format!("Failed to delete topic: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(MqError::TopicNotFound(name.to_string()));
        }

        Ok(())
    }

    // ===== Subscription Management =====

    async fn create_subscription(&self, request: CreateSubscriptionRequest) -> Result<Subscription> {
        // Verify topic exists
        if self.get_topic(&request.topic_name).await?.is_none() {
            return Err(MqError::TopicNotFound(request.topic_name));
        }

        let subscription = Subscription {
            id: Uuid::new_v4(),
            name: request.name,
            topic_name: request.topic_name,
            filter: request.filter,
            created_at: Utc::now(),
            config: request.config,
        };

        sqlx::query(
            r#"
            INSERT INTO subscriptions (id, name, topic_name, filter, created_at, config)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(&subscription.id)
        .bind(&subscription.name)
        .bind(&subscription.topic_name)
        .bind(&subscription.filter)
        .bind(&subscription.created_at)
        .bind(serde_json::to_value(&subscription.config)?)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to create subscription: {}", e)))?;

        Ok(subscription)
    }

    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>> {
        let subscription = sqlx::query_as::<_, (Uuid, String, String, Option<String>, chrono::DateTime<Utc>, serde_json::Value)>(
            "SELECT id, name, topic_name, filter, created_at, config FROM subscriptions WHERE name = $1"
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get subscription: {}", e)))?
        .map(|(id, name, topic_name, filter, created_at, config)| Subscription {
            id,
            name,
            topic_name,
            filter,
            created_at,
            config: serde_json::from_value(config).unwrap_or_default(),
        });

        Ok(subscription)
    }

    async fn list_subscriptions(&self, topic_name: Option<&str>) -> Result<Vec<Subscription>> {
        let subscriptions = if let Some(topic) = topic_name {
            sqlx::query_as::<_, (Uuid, String, String, Option<String>, chrono::DateTime<Utc>, serde_json::Value)>(
                "SELECT id, name, topic_name, filter, created_at, config FROM subscriptions WHERE topic_name = $1 ORDER BY created_at DESC"
            )
            .bind(topic)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, (Uuid, String, String, Option<String>, chrono::DateTime<Utc>, serde_json::Value)>(
                "SELECT id, name, topic_name, filter, created_at, config FROM subscriptions ORDER BY created_at DESC"
            )
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| MqError::DatabaseError(format!("Failed to list subscriptions: {}", e)))?
        .into_iter()
        .map(|(id, name, topic_name, filter, created_at, config)| Subscription {
            id,
            name,
            topic_name,
            filter,
            created_at,
            config: serde_json::from_value(config).unwrap_or_default(),
        })
        .collect();

        Ok(subscriptions)
    }

    async fn delete_subscription(&self, name: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM subscriptions WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| MqError::DatabaseError(format!("Failed to delete subscription: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(MqError::SubscriptionNotFound(name.to_string()));
        }

        Ok(())
    }

    // ===== Client Management =====

    async fn register_client(&self, request: RegisterClientRequest) -> Result<Client> {
        // Verify subscription exists
        if self.get_subscription(&request.subscription_name).await?.is_none() {
            return Err(MqError::SubscriptionNotFound(request.subscription_name));
        }

        let client = Client {
            id: Uuid::new_v4(),
            name: request.name,
            webhook_url: request.webhook_url,
            subscription_name: request.subscription_name,
            auth_headers: request.auth_headers,
            created_at: Utc::now(),
            active: true,
        };

        sqlx::query(
            r#"
            INSERT INTO clients (id, name, webhook_url, subscription_name, auth_headers, created_at, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&client.id)
        .bind(&client.name)
        .bind(&client.webhook_url)
        .bind(&client.subscription_name)
        .bind(serde_json::to_value(&client.auth_headers)?)
        .bind(&client.created_at)
        .bind(&client.active)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to register client: {}", e)))?;

        Ok(client)
    }

    async fn get_client(&self, id: Uuid) -> Result<Option<Client>> {
        let client = sqlx::query_as::<_, (Uuid, String, String, String, serde_json::Value, chrono::DateTime<Utc>, bool)>(
            "SELECT id, name, webhook_url, subscription_name, auth_headers, created_at, active FROM clients WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get client: {}", e)))?
        .map(|(id, name, webhook_url, subscription_name, auth_headers, created_at, active)| Client {
            id,
            name,
            webhook_url,
            subscription_name,
            auth_headers: serde_json::from_value(auth_headers).unwrap_or_default(),
            created_at,
            active,
        });

        Ok(client)
    }

    async fn list_clients(&self, subscription_name: Option<&str>) -> Result<Vec<Client>> {
        let clients = if let Some(sub) = subscription_name {
            sqlx::query_as::<_, (Uuid, String, String, String, serde_json::Value, chrono::DateTime<Utc>, bool)>(
                "SELECT id, name, webhook_url, subscription_name, auth_headers, created_at, active FROM clients WHERE subscription_name = $1 ORDER BY created_at DESC"
            )
            .bind(sub)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query_as::<_, (Uuid, String, String, String, serde_json::Value, chrono::DateTime<Utc>, bool)>(
                "SELECT id, name, webhook_url, subscription_name, auth_headers, created_at, active FROM clients ORDER BY created_at DESC"
            )
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| MqError::DatabaseError(format!("Failed to list clients: {}", e)))?
        .into_iter()
        .map(|(id, name, webhook_url, subscription_name, auth_headers, created_at, active)| Client {
            id,
            name,
            webhook_url,
            subscription_name,
            auth_headers: serde_json::from_value(auth_headers).unwrap_or_default(),
            created_at,
            active,
        })
        .collect();

        Ok(clients)
    }

    async fn update_client_status(&self, id: Uuid, active: bool) -> Result<()> {
        let result = sqlx::query("UPDATE clients SET active = $1 WHERE id = $2")
            .bind(active)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| MqError::DatabaseError(format!("Failed to update client status: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(MqError::ClientNotFound(id.to_string()));
        }

        Ok(())
    }

    async fn delete_client(&self, id: Uuid) -> Result<()> {
        let result = sqlx::query("DELETE FROM clients WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| MqError::DatabaseError(format!("Failed to delete client: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(MqError::ClientNotFound(id.to_string()));
        }

        Ok(())
    }

    // ===== Event Publishing =====

    async fn publish_event(&self, event: Event) -> Result<Event> {
        sqlx::query(
            r#"
            INSERT INTO events (id, topic, payload, metadata, created_at, event_type)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(&event.id)
        .bind(&event.topic)
        .bind(&event.payload)
        .bind(&event.metadata)
        .bind(&event.created_at)
        .bind(&event.event_type)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to publish event: {}", e)))?;

        Ok(event)
    }

    // ===== Event Consumption =====

    async fn poll_events(&self, subscription_name: &str, batch_size: usize) -> Result<Vec<Event>> {
        let events = sqlx::query_as::<_, (Uuid, String, serde_json::Value, serde_json::Value, chrono::DateTime<Utc>, Option<String>)>(
            r#"
            SELECT DISTINCT e.id, e.topic, e.payload, e.metadata, e.created_at, e.event_type
            FROM events e
            INNER JOIN subscriptions s ON s.topic_name = e.topic
            WHERE s.name = $1
            ORDER BY e.created_at ASC
            LIMIT $2
            "#,
        )
        .bind(subscription_name)
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to poll events: {}", e)))?
        .into_iter()
        .map(|(id, topic, payload, metadata, created_at, event_type)| Event {
            id,
            topic,
            payload,
            metadata,
            created_at,
            event_type,
        })
        .collect();

        Ok(events)
    }

    async fn acknowledge_event(&self, event_id: Uuid, client_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE event_deliveries SET status = 'Success', completed_at = NOW() WHERE event_id = $1 AND client_id = $2"
        )
        .bind(event_id)
        .bind(client_id)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to acknowledge event: {}", e)))?;

        Ok(())
    }

    async fn nack_event(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        _error_message: String,
    ) -> Result<()> {
        // Get subscription config for retry logic
        let config = sqlx::query_as::<_, (serde_json::Value,)>(
            r#"
            SELECT s.config
            FROM event_deliveries ed
            INNER JOIN subscriptions s ON s.name = ed.subscription_name
            WHERE ed.event_id = $1 AND ed.client_id = $2
            "#,
        )
        .bind(event_id)
        .bind(client_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get subscription config: {}", e)))?;

        if let Some((config_json,)) = config {
            let config: pmp_mq_core::SubscriptionConfig =
                serde_json::from_value(config_json).unwrap_or_default();

            // Increment attempt count and update status
            sqlx::query(
                r#"
                UPDATE event_deliveries
                SET attempt_count = attempt_count + 1,
                    last_attempt_at = NOW(),
                    next_retry_at = NOW() + INTERVAL '1 minute' * POW(2, attempt_count),
                    status = CASE
                        WHEN attempt_count + 1 >= $3 THEN 'DeadLetter'
                        ELSE 'Failed'
                    END
                WHERE event_id = $1 AND client_id = $2
                "#,
            )
            .bind(event_id)
            .bind(client_id)
            .bind(config.max_delivery_attempts)
            .execute(&self.pool)
            .await
            .map_err(|e| MqError::DatabaseError(format!("Failed to nack event: {}", e)))?;
        }

        Ok(())
    }

    // ===== Delivery Tracking =====

    async fn record_delivery_attempt(&self, attempt: DeliveryAttempt) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO delivery_attempts (id, event_id, client_id, attempt_number, status, attempted_at, completed_at, error_message, http_status_code)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(&attempt.id)
        .bind(&attempt.event_id)
        .bind(&attempt.client_id)
        .bind(attempt.attempt_number)
        .bind(format!("{:?}", attempt.status))
        .bind(&attempt.attempted_at)
        .bind(&attempt.completed_at)
        .bind(&attempt.error_message)
        .bind(attempt.http_status_code.map(|c| c as i16))
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to record delivery attempt: {}", e)))?;

        Ok(())
    }

    async fn get_delivery_attempts(&self, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
        let attempts = sqlx::query_as::<_, (Uuid, Uuid, Uuid, i32, String, chrono::DateTime<Utc>, Option<chrono::DateTime<Utc>>, Option<String>, Option<i16>)>(
            r#"
            SELECT id, event_id, client_id, attempt_number, status, attempted_at, completed_at, error_message, http_status_code
            FROM delivery_attempts
            WHERE event_id = $1
            ORDER BY attempt_number ASC
            "#,
        )
        .bind(event_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get delivery attempts: {}", e)))?
        .into_iter()
        .map(|(id, event_id, client_id, attempt_number, status, attempted_at, completed_at, error_message, http_status_code)| {
            DeliveryAttempt {
                id,
                event_id,
                client_id,
                attempt_number,
                status: match status.as_str() {
                    "Pending" => DeliveryStatus::Pending,
                    "InProgress" => DeliveryStatus::InProgress,
                    "Success" => DeliveryStatus::Success,
                    "Failed" => DeliveryStatus::Failed,
                    "DeadLetter" => DeliveryStatus::DeadLetter,
                    _ => DeliveryStatus::Failed,
                },
                attempted_at,
                completed_at,
                error_message,
                http_status_code: http_status_code.map(|c| c as u16),
            }
        })
        .collect();

        Ok(attempts)
    }

    async fn get_pending_deliveries(
        &self,
        subscription_name: &str,
        limit: usize,
    ) -> Result<Vec<(Event, Client)>> {
        let results = sqlx::query_as::<_, (
            Uuid, String, serde_json::Value, serde_json::Value, chrono::DateTime<Utc>, Option<String>,
            Uuid, String, String, String, serde_json::Value, chrono::DateTime<Utc>, bool,
        )>(
            r#"
            SELECT
                e.id, e.topic, e.payload, e.metadata, e.created_at, e.event_type,
                c.id, c.name, c.webhook_url, c.subscription_name, c.auth_headers, c.created_at, c.active
            FROM event_deliveries ed
            INNER JOIN events e ON e.id = ed.event_id
            INNER JOIN clients c ON c.id = ed.client_id
            WHERE ed.subscription_name = $1
              AND ed.status IN ('Pending', 'Failed')
              AND (ed.next_retry_at IS NULL OR ed.next_retry_at <= NOW())
              AND c.active = true
            ORDER BY e.created_at ASC
            LIMIT $2
            "#,
        )
        .bind(subscription_name)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to get pending deliveries: {}", e)))?;

        let deliveries = results
            .into_iter()
            .map(|(event_id, topic, payload, metadata, created_at, event_type, client_id, name, webhook_url, subscription_name, auth_headers, client_created_at, active)| {
                let event = Event {
                    id: event_id,
                    topic,
                    payload,
                    metadata,
                    created_at,
                    event_type,
                };
                let client = Client {
                    id: client_id,
                    name,
                    webhook_url,
                    subscription_name,
                    auth_headers: serde_json::from_value(auth_headers).unwrap_or_default(),
                    created_at: client_created_at,
                    active,
                };
                (event, client)
            })
            .collect();

        Ok(deliveries)
    }

    async fn update_delivery_status(
        &self,
        event_id: Uuid,
        client_id: Uuid,
        status: DeliveryStatus,
    ) -> Result<()> {
        let status_str = format!("{:?}", status);
        sqlx::query(
            "UPDATE event_deliveries SET status = $1 WHERE event_id = $2 AND client_id = $3"
        )
        .bind(status_str)
        .bind(event_id)
        .bind(client_id)
        .execute(&self.pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to update delivery status: {}", e)))?;

        Ok(())
    }

    // ===== Health Check =====

    async fn health_check(&self) -> Result<bool> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| true)
            .map_err(|e| MqError::DatabaseError(format!("Health check failed: {}", e)))
    }
}
