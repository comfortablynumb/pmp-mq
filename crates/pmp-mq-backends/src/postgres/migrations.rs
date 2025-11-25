use pmp_mq_core::{MqError, Result};
use sqlx::PgPool;
use tracing::info;

const MIGRATIONS: &[&str] = &[
    // Migration 1: Create topics table
    r#"
    CREATE TABLE IF NOT EXISTS topics (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        description TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        config JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_topics_name ON topics(name);
    "#,
    // Migration 2: Create subscriptions table
    r#"
    CREATE TABLE IF NOT EXISTS subscriptions (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        topic_name VARCHAR(255) NOT NULL,
        filter TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        FOREIGN KEY (topic_name) REFERENCES topics(name) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_subscriptions_name ON subscriptions(name);
    CREATE INDEX IF NOT EXISTS idx_subscriptions_topic ON subscriptions(topic_name);
    "#,
    // Migration 3: Create clients table
    r#"
    CREATE TABLE IF NOT EXISTS clients (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        webhook_url TEXT NOT NULL,
        subscription_name VARCHAR(255) NOT NULL,
        auth_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        active BOOLEAN NOT NULL DEFAULT true,
        FOREIGN KEY (subscription_name) REFERENCES subscriptions(name) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_clients_subscription ON clients(subscription_name);
    CREATE INDEX IF NOT EXISTS idx_clients_active ON clients(active);
    "#,
    // Migration 4: Create events table
    r#"
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY,
        topic VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        event_type VARCHAR(255),
        FOREIGN KEY (topic) REFERENCES topics(name) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic);
    CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
    "#,
    // Migration 5: Create event_deliveries table
    r#"
    CREATE TABLE IF NOT EXISTS event_deliveries (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_id UUID NOT NULL,
        client_id UUID NOT NULL,
        subscription_name VARCHAR(255) NOT NULL,
        status VARCHAR(50) NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_attempt_at TIMESTAMPTZ,
        next_retry_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
        UNIQUE(event_id, client_id)
    );
    CREATE INDEX IF NOT EXISTS idx_deliveries_status ON event_deliveries(status);
    CREATE INDEX IF NOT EXISTS idx_deliveries_next_retry ON event_deliveries(next_retry_at);
    CREATE INDEX IF NOT EXISTS idx_deliveries_subscription ON event_deliveries(subscription_name);
    "#,
    // Migration 6: Create delivery_attempts table
    r#"
    CREATE TABLE IF NOT EXISTS delivery_attempts (
        id UUID PRIMARY KEY,
        event_id UUID NOT NULL,
        client_id UUID NOT NULL,
        attempt_number INTEGER NOT NULL,
        status VARCHAR(50) NOT NULL,
        attempted_at TIMESTAMPTZ NOT NULL,
        completed_at TIMESTAMPTZ,
        error_message TEXT,
        http_status_code SMALLINT,
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_attempts_event ON delivery_attempts(event_id);
    CREATE INDEX IF NOT EXISTS idx_attempts_client ON delivery_attempts(client_id);
    "#,
    // Migration 7: Create a trigger to auto-create deliveries when events are published
    r#"
    CREATE OR REPLACE FUNCTION create_event_deliveries()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO event_deliveries (event_id, client_id, subscription_name, status, created_at)
        SELECT
            NEW.id,
            c.id,
            s.name,
            'Pending',
            NOW()
        FROM subscriptions s
        INNER JOIN clients c ON c.subscription_name = s.name
        WHERE s.topic_name = NEW.topic
          AND c.active = true;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS trigger_create_deliveries ON events;
    CREATE TRIGGER trigger_create_deliveries
        AFTER INSERT ON events
        FOR EACH ROW
        EXECUTE FUNCTION create_event_deliveries();
    "#,
    // Migration 8: Create event_metadata table for lightweight event tracking
    r#"
    CREATE TABLE IF NOT EXISTS event_metadata (
        id UUID PRIMARY KEY,
        topic VARCHAR(255) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        event_type VARCHAR(255),
        payload JSONB NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_event_metadata_topic ON event_metadata(topic);
    CREATE INDEX IF NOT EXISTS idx_event_metadata_created_at ON event_metadata(created_at);

    -- Remove foreign key constraint on event_deliveries.event_id since events might not be in events table
    ALTER TABLE event_deliveries DROP CONSTRAINT IF EXISTS event_deliveries_event_id_fkey;

    -- Remove foreign key constraint on delivery_attempts.event_id
    ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_event_id_fkey;
    "#,
];

pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    info!("Running database migrations...");

    // Create migrations tracking table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _migrations (
            id SERIAL PRIMARY KEY,
            version INTEGER NOT NULL UNIQUE,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await
    .map_err(|e| MqError::DatabaseError(format!("Failed to create migrations table: {}", e)))?;

    // Check which migrations have been applied
    let applied: Vec<i32> = sqlx::query_scalar("SELECT version FROM _migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .map_err(|e| MqError::DatabaseError(format!("Failed to query migrations: {}", e)))?;

    // Apply missing migrations
    for (idx, migration) in MIGRATIONS.iter().enumerate() {
        let version = idx as i32;
        if !applied.contains(&version) {
            info!("Applying migration {}", version);

            // Execute migration
            sqlx::query(migration).execute(pool).await.map_err(|e| {
                MqError::DatabaseError(format!("Failed to apply migration {}: {}", version, e))
            })?;

            // Record migration
            sqlx::query("INSERT INTO _migrations (version) VALUES ($1)")
                .bind(version)
                .execute(pool)
                .await
                .map_err(|e| {
                    MqError::DatabaseError(format!("Failed to record migration {}: {}", version, e))
                })?;
        }
    }

    info!("Database migrations completed");
    Ok(())
}
