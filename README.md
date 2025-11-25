# PMP Message Queue

**Poor Man's Platform Message Queue** - A simple, HTTP-based message queue abstraction that allows you to use different backends (Kafka, PostgreSQL, AWS SQS) for pub/sub messaging with webhook delivery.

## Features

- 🚀 **Multiple Backends**: Support for PostgreSQL, Kafka, and AWS SQS
- 📡 **HTTP API**: REST API for publishing and subscribing to events
- 🔔 **Webhook Delivery**: Automatic HTTP webhook delivery to registered clients
- 🔄 **Retry Logic**: Configurable retry with exponential backoff
- 📊 **Delivery Tracking**: Track delivery attempts and status
- ⚡ **Batch Publishing**: Publish multiple events in a single request
- 🔍 **Event Filtering**: JSONPath and regex-based event filtering on subscriptions
- 📈 **Prometheus Metrics**: Comprehensive metrics exporter for monitoring
- 🔬 **Dead Letter Queue**: Full DLQ management with retry and deletion
- 🎨 **Admin Dashboard**: Web-based dashboard for monitoring and management
- 💾 **Configurable Storage**: Opt-in event storage (disabled by default for performance)
- 🐳 **Docker Support**: Ready-to-use Docker Compose setup
- 🧪 **Integration Tests**: Comprehensive test suite for all features
- ⚙️ **CI/CD Ready**: GitHub Actions workflows for testing and deployment
- 🏗️ **Modular Architecture**: Clean separation between core, backends, and server

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                   HTTP API (Axum)                        │
│  POST /events/publish   POST /events/batch               │
│  GET /topics   GET /metrics   GET /clients               │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│              Backend Abstraction (Trait)                 │
└──────┬──────────────────┬────────────────┬───────────────┘
       │                  │                │
┌──────▼────────┐  ┌──────▼──────┐  ┌─────▼──────┐
│  PostgreSQL   │  │    Kafka    │  │  AWS SQS   │
│   Backend     │  │   Backend   │  │  Backend   │
└───────────────┘  └─────────────┘  └────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────┐
│           Webhook Delivery Worker                        │
│  Polls pending events and delivers via HTTP webhooks    │
│  Configurable: batch size, concurrency, retry logic     │
└──────────────────────────────────────────────────────────┘
```

## Quick Start

### Using Docker Compose

1. **Start with PostgreSQL backend:**
```bash
docker-compose up postgres pmp-mq-postgres
```

2. **Or start with Kafka backend:**
```bash
docker-compose up postgres kafka zookeeper pmp-mq-kafka
```

The API will be available at:
- PostgreSQL backend: `http://localhost:8080`
- Kafka backend: `http://localhost:8081`

### Building from Source

1. **Prerequisites:**
   - Rust 1.70+
   - PostgreSQL (if using Postgres backend)
   - Kafka + Zookeeper (if using Kafka backend)

2. **Build:**
```bash
cargo build --release
```

3. **Run:**
```bash
# With PostgreSQL backend
export DATABASE_URL=postgres://postgres:postgres@localhost/pmp_mq
export BACKEND_TYPE=postgres
cargo run --release --bin pmp-mq-server

# With Kafka backend
export BACKEND_TYPE=kafka
export PMP_MQ__KAFKA__BROKERS=localhost:9092
export DATABASE_URL=postgres://postgres:postgres@localhost/pmp_mq
cargo run --release --bin pmp-mq-server
```

## Usage Examples

### 1. Create a Topic

```bash
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "user.events",
    "description": "User activity events"
  }'
```

### 2. Create a Subscription

```bash
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "user-events-sub",
    "topic_name": "user.events",
    "config": {
      "ack_timeout_seconds": 300,
      "max_delivery_attempts": 5
    }
  }'
```

### 3. Register a Client (Webhook Listener)

```bash
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-service",
    "webhook_url": "https://my-service.example.com/webhooks/events",
    "subscription_name": "user-events-sub",
    "auth_headers": {
      "Authorization": "Bearer your-token-here"
    }
  }'
```

### 4. Publish an Event

```bash
curl -X POST http://localhost:8080/api/v1/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.events",
    "event_type": "user.created",
    "payload": {
      "user_id": "123",
      "email": "user@example.com",
      "action": "signup"
    },
    "metadata": {
      "source": "api",
      "version": "1.0"
    }
  }'
```

The system will automatically deliver this event to all registered clients via their webhook URLs.

### 5. List Topics

```bash
curl http://localhost:8080/api/v1/topics
```

### 6. Batch Publish Events (NEW!)

Publish multiple events in a single request for better performance:

```bash
curl -X POST http://localhost:8080/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "user.events",
        "event_type": "user.signup",
        "payload": {"user_id": "123", "email": "user1@example.com"}
      },
      {
        "topic": "user.events",
        "event_type": "user.login",
        "payload": {"user_id": "456", "email": "user2@example.com"}
      }
    ]
  }'
```

Response includes success/failure breakdown:
```json
{
  "total": 2,
  "success_count": 2,
  "failure_count": 0,
  "published": [
    {"event_id": "...", "topic": "user.events", "published_at": "..."},
    {"event_id": "...", "topic": "user.events", "published_at": "..."}
  ],
  "failed": []
}
```

### 7. Get System Metrics (NEW!)

Monitor system health and performance:

```bash
curl http://localhost:8080/api/v1/metrics
```

Response:
```json
{
  "topics_count": 5,
  "subscriptions_count": 8,
  "clients_count": 12,
  "active_clients_count": 10,
  "pending_deliveries": 150,
  "failed_deliveries": 3,
  "dead_letter_count": 1
}
```

### 8. Create Topic with Custom Storage (NEW!)

By default, events are NOT stored in the database for better performance. Enable storage per-topic:

```bash
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "audit.events",
    "description": "Audit trail - keep all events",
    "config": {
      "store_events": true,
      "retention_seconds": 2592000
    }
  }'
```

**Storage Modes:**
- `store_events: false` (default) - Only delivery tracking, minimal storage
- `store_events: true` - Full event storage for replay and audit

### 9. Get Delivery Attempts

```bash
curl http://localhost:8080/api/v1/events/{event_id}/attempts
```

## API Reference

### Topics

- `POST /api/v1/topics` - Create a new topic
- `GET /api/v1/topics` - List all topics
- `GET /api/v1/topics/:name` - Get a specific topic
- `DELETE /api/v1/topics/:name` - Delete a topic

### Subscriptions

- `POST /api/v1/subscriptions` - Create a new subscription
- `GET /api/v1/subscriptions` - List all subscriptions (optional `?topic=name` filter)
- `GET /api/v1/subscriptions/:name` - Get a specific subscription
- `DELETE /api/v1/subscriptions/:name` - Delete a subscription

### Clients

- `POST /api/v1/clients` - Register a new client
- `GET /api/v1/clients` - List all clients (optional `?subscription=name` filter)
- `GET /api/v1/clients/:id` - Get a specific client
- `PUT /api/v1/clients/:id/status` - Update client status (activate/deactivate)
- `DELETE /api/v1/clients/:id` - Delete a client

### Events

- `POST /api/v1/events/publish` - Publish an event
- `POST /api/v1/events/batch` - Batch publish multiple events (NEW!)
- `GET /api/v1/events/:id/attempts` - Get delivery attempts for an event

### Metrics

- `GET /api/v1/metrics` - Get system metrics (NEW!)

### Health

- `GET /api/v1/health` - Health check

## Configuration

Configuration can be provided via environment variables or a `config.toml` file.

### Environment Variables

```bash
# Backend selection
BACKEND_TYPE=postgres  # Options: postgres, kafka, sqs

# PostgreSQL configuration
DATABASE_URL=postgres://postgres:postgres@localhost/pmp_mq

# Kafka configuration (when using Kafka backend)
PMP_MQ__KAFKA__BROKERS=localhost:9092
PMP_MQ__KAFKA__POSTGRES_URL=postgres://postgres:postgres@localhost/pmp_mq

# AWS SQS configuration (when using SQS backend)
PMP_MQ__SQS__REGION=us-east-1
PMP_MQ__SQS__QUEUE_PREFIX=pmp-mq-
PMP_MQ__SQS__POSTGRES_URL=postgres://postgres:postgres@localhost/pmp_mq

# Server configuration
HOST=0.0.0.0
PORT=8080

# Delivery worker configuration
PMP_MQ__DELIVERY__WORKER_INTERVAL_SECS=5
PMP_MQ__DELIVERY__BATCH_SIZE=100
PMP_MQ__DELIVERY__MAX_CONCURRENT_DELIVERIES=50
PMP_MQ__DELIVERY__REQUEST_TIMEOUT_SECS=30
```

### Config File (config.toml)

```toml
backend_type = "postgres"

[server]
host = "0.0.0.0"
port = 8080

[postgres]
database_url = "postgres://postgres:postgres@localhost/pmp_mq"
max_connections = 10

[sqs]
region = "us-east-1"
queue_prefix = "pmp-mq-"
postgres_url = "postgres://postgres:postgres@localhost/pmp_mq"
max_connections = 10

[kafka]
brokers = "localhost:9092"
postgres_url = "postgres://postgres:postgres@localhost/pmp_mq"
max_connections = 10

[delivery]
worker_interval_secs = 5
batch_size = 100
max_concurrent_deliveries = 50
request_timeout_secs = 30
```

## Backend Comparison

### PostgreSQL Backend

**Pros:**
- Simple setup
- Good for low to medium throughput
- Strong consistency guarantees
- Integrated metadata and event storage

**Cons:**
- Limited scalability for high throughput
- Not ideal for event streaming use cases

**Best for:** Small to medium applications, prototyping, when simplicity is more important than extreme scalability.

### Kafka Backend

**Pros:**
- Excellent for high throughput
- Built for streaming and event sourcing
- Horizontal scalability
- Strong durability guarantees

**Cons:**
- More complex setup (requires Zookeeper)
- Uses PostgreSQL for metadata anyway
- Higher operational overhead

**Best for:** High-throughput applications, event streaming, microservices architectures.

### AWS SQS Backend (NEW!)

**Pros:**
- Fully managed service (no infrastructure management)
- Built-in scalability and availability
- Pay-per-use pricing model
- Integrates with AWS ecosystem
- Simple setup with AWS credentials

**Cons:**
- Requires AWS account
- Uses PostgreSQL for metadata storage
- AWS-specific (vendor lock-in)
- Additional costs for AWS services

**Best for:** AWS-based applications, teams wanting managed infrastructure, applications needing auto-scaling without operational overhead.

## Webhook Payload Format

When delivering events to webhook URLs, the system sends a POST request with the following JSON payload:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "topic": "user.events",
  "event_type": "user.created",
  "payload": {
    "user_id": "123",
    "email": "user@example.com"
  },
  "metadata": {
    "source": "api",
    "version": "1.0"
  },
  "created_at": "2025-11-14T10:30:00Z"
}
```

### Webhook Requirements

Your webhook endpoint should:
1. Accept POST requests
2. Return a 2xx status code for successful delivery
3. Process the event idempotently (same event may be delivered multiple times)
4. Respond within the configured timeout (default: 30s)

## Retry Logic

- Failed deliveries are retried with exponential backoff
- Default maximum attempts: 5
- After max attempts, events move to "DeadLetter" status
- Configurable per subscription via `max_delivery_attempts`

## Event Filtering

Subscriptions support advanced event filtering using JSONPath expressions and logical operators:

### Filter Syntax

```bash
# Event type filter
"event_type == 'user.created'"

# Topic pattern (regex)
"topic matches 'user\\..*'"

# JSONPath filter
"$.payload.action == 'signup'"

# Numeric comparison
"$.payload.age > 18"

# String contains
"$.payload.email contains '@example.com'"

# Regex match
"$.payload.username matches '^admin.*'"

# Logical operators
"event_type == 'user.created' AND $.payload.verified == true"
"$.payload.role == 'admin' OR $.payload.role == 'moderator'"
```

### Creating a Subscription with Filter

```bash
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "verified-signups",
    "topic_name": "user.events",
    "filter": "event_type == '\''user.created'\'' AND $.action == '\''signup'\'' AND $.verified == true"
  }'
```

Only events matching the filter will be delivered to clients on this subscription.

## Prometheus Metrics

PMP-MQ exports Prometheus-compatible metrics for monitoring:

### Available Metrics

- `events_published_total` - Total events published by topic
- `events_delivered_total` - Total successful deliveries by subscription
- `events_filtered_total` - Events filtered out by subscription
- `delivery_attempts_total` - Total delivery attempts by status
- `delivery_latency_seconds` - Delivery latency histogram
- `delivery_failures_total` - Failed deliveries by reason
- `pending_deliveries` - Current pending deliveries gauge
- `dead_letter_queue_size` - Current DLQ size gauge
- `topics_total`, `subscriptions_total`, `clients_total` - Resource counts
- `http_requests_total`, `http_request_duration_seconds` - HTTP metrics

### Endpoint

```bash
curl http://localhost:8080/api/v1/metrics/prometheus
```

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'pmp-mq'
    static_configs:
      - targets: ['pmp-mq-server:8080']
    metrics_path: '/api/v1/metrics/prometheus'
    scrape_interval: 15s
```

## Dead Letter Queue Management

Manage failed deliveries through DLQ endpoints:

### List DLQ Events

```bash
curl "http://localhost:8080/api/v1/dlq/events?limit=50&offset=0&subscription_name=my-subscription"
```

### Retry Single Event

```bash
curl -X POST "http://localhost:8080/api/v1/dlq/events/{event_id}/{client_id}/retry"
```

### Delete Single Event

```bash
curl -X DELETE "http://localhost:8080/api/v1/dlq/events/{event_id}/{client_id}"
```

### Bulk Retry

```bash
curl -X POST http://localhost:8080/api/v1/dlq/bulk-retry \
  -H "Content-Type: application/json" \
  -d '{"limit": 100, "subscription_name": "my-subscription"}'
```

### Bulk Delete

```bash
curl -X POST http://localhost:8080/api/v1/dlq/bulk-delete \
  -H "Content-Type: application/json" \
  -d '{"limit": 100}'
```

## Admin Dashboard

A simple web-based admin dashboard is available in the `dashboard/` directory.

### Features
- Real-time metrics with auto-refresh
- Topic, subscription, and client management
- DLQ inspection and management
- Event publishing interface

### Running the Dashboard

```bash
# Serve with Python
cd dashboard
python3 -m http.server 8081

# Or with Node.js
cd dashboard
npx serve -p 8081
```

Then open http://localhost:8081 in your browser.

See [`dashboard/README.md`](dashboard/README.md) for full documentation.

## Webhook Receiver Examples

Example webhook receivers are provided in the `examples/webhook-receivers/` directory:

- **Python (FastAPI)**: `examples/webhook-receivers/python_fastapi.py`
- **Node.js (Express)**: `examples/webhook-receivers/nodejs_express.js`

Both examples demonstrate:
- Idempotent event processing
- Event type handling
- Error handling with proper HTTP status codes
- Event storage and listing
- Health check endpoints

See [`examples/webhook-receivers/README.md`](examples/webhook-receivers/README.md) for usage.

## Development

### Project Structure

```
pmp-mq/
├── crates/
│   ├── pmp-mq-core/        # Core types and abstractions
│   ├── pmp-mq-backends/    # Backend implementations
│   └── pmp-mq-server/      # HTTP server and delivery worker
├── tests/
│   └── integration/        # Integration tests
├── examples/
│   └── webhook-receivers/  # Example webhook receivers (Python, Node.js)
├── dashboard/              # Web-based admin dashboard
├── .github/
│   └── workflows/          # CI/CD workflows
├── Cargo.toml              # Workspace configuration
├── docker-compose.yml      # Docker setup
├── CHANGELOG.md            # Version history and changelog
└── README.md               # This file
```

### Running Tests

```bash
cargo test --all
```

### Running with Logging

```bash
RUST_LOG=debug cargo run --bin pmp-mq-server
```

## Contributing

Contributions are welcome! This is part of the Poor Man's Platform ecosystem.

## License

MIT License - see LICENSE file for details

## Part of Poor Man's Platform

PMP-MQ is part of the Poor Man's Platform ecosystem - building simple, cost-effective alternatives to complex cloud services.
