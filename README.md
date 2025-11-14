# PMP Message Queue

**Poor Man's Platform Message Queue** - A simple, HTTP-based message queue abstraction that allows you to use different backends (Kafka, PostgreSQL) for pub/sub messaging with webhook delivery.

## Features

- 🚀 **Multiple Backends**: Support for PostgreSQL and Kafka
- 📡 **HTTP API**: REST API for publishing and subscribing to events
- 🔔 **Webhook Delivery**: Automatic HTTP webhook delivery to registered clients
- 🔄 **Retry Logic**: Configurable retry with exponential backoff
- 📊 **Delivery Tracking**: Track delivery attempts and status
- 🐳 **Docker Support**: Ready-to-use Docker Compose setup
- 🏗️ **Modular Architecture**: Clean separation between core, backends, and server

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   HTTP API (Axum)                   │
│  POST /events/publish   GET /topics  GET /clients  │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│              Backend Abstraction (Trait)            │
└──────────────┬─────────────────┬────────────────────┘
               │                 │
      ┌────────▼────────┐   ┌───▼──────────┐
      │   PostgreSQL    │   │    Kafka     │
      │     Backend     │   │   Backend    │
      └─────────────────┘   └──────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│           Webhook Delivery Worker                   │
│  Polls pending events and delivers to webhooks     │
└─────────────────────────────────────────────────────┘
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

### 6. Get Delivery Attempts

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
- `GET /api/v1/events/:id/attempts` - Get delivery attempts for an event

### Health

- `GET /api/v1/health` - Health check

## Configuration

Configuration can be provided via environment variables or a `config.toml` file.

### Environment Variables

```bash
# Backend selection
BACKEND_TYPE=postgres  # or 'kafka'

# PostgreSQL configuration
DATABASE_URL=postgres://postgres:postgres@localhost/pmp_mq

# Kafka configuration (when using Kafka backend)
PMP_MQ__KAFKA__BROKERS=localhost:9092
PMP_MQ__KAFKA__POSTGRES_URL=postgres://postgres:postgres@localhost/pmp_mq

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

## Development

### Project Structure

```
pmp-mq/
├── crates/
│   ├── pmp-mq-core/        # Core types and abstractions
│   ├── pmp-mq-backends/    # Backend implementations
│   └── pmp-mq-server/      # HTTP server and delivery worker
├── Cargo.toml              # Workspace configuration
├── docker-compose.yml      # Docker setup
└── README.md
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
