# Changelog

## [Unreleased]

### Added
- **AWS SQS Backend Support**: New backend implementation using AWS SQS for scalable message queuing
  - Auto-creates SQS queues for topics
  - Uses PostgreSQL for metadata storage
  - Configurable queue prefix and region

- **Configurable Event Storage** (Default: Disabled)
  - Events are no longer stored in database by default for better performance
  - Set `store_events: true` in topic config to enable full event storage
  - Lightweight event metadata always stored for delivery tracking
  - Reduces database storage requirements significantly

- **Batch Event Publishing**
  - New endpoint: `POST /api/v1/events/batch`
  - Publish multiple events in a single request
  - Returns detailed success/failure breakdown per event
  - Improves throughput for high-volume scenarios

- **System Metrics Endpoint**
  - New endpoint: `GET /api/v1/metrics`
  - Returns system-wide statistics:
    - Topic, subscription, and client counts
    - Active client count
    - Delivery status metrics

- **Event Metadata Table**
  - Separate lightweight table for event tracking when full storage is disabled
  - Removes foreign key constraints for better scalability
  - Supports hybrid queries across both storage modes

### Changed
- Topic configuration now includes `store_events` flag (default: false)
- Backend initialization supports three types: postgres, kafka, sqs
- Database migrations updated to support dual event storage modes

### Performance Improvements
- Reduced database storage footprint with configurable event storage
- Batch publishing reduces HTTP overhead
- SQS backend provides horizontal scalability

## Suggested Future Enhancements

### 1. Event Filtering & Routing
- **Content-based filtering**: Filter events by payload attributes
- **Header-based routing**: Route based on metadata/headers
- **JSONPath expressions**: Advanced filtering capabilities
Example:
```json
{
  "name": "my-subscription",
  "topic_name": "user.events",
  "filter": "$.payload.action == 'signup' && $.payload.region == 'US'"
}
```

### 2. Dead Letter Queue Management
- **DLQ inspection endpoints**: View failed events
- **Retry from DLQ**: Manual or automated retry
- **DLQ archival**: Archive old dead letters

### 3. Event Replay
- **Time-based replay**: Replay events from specific time range
- **Event ID replay**: Replay from specific event
- **Subscription-level replay**: Replay for specific subscription

### 4. Webhook Security
- **Signature verification**: HMAC signatures for webhooks
- **mTLS support**: Mutual TLS for secure delivery
- **IP whitelisting**: Restrict client webhook URLs

### 5. Rate Limiting
- **Per-client rate limits**: Limit delivery rate per client
- **Topic-level limits**: Control publishing rate
- **Backpressure**: Queue when clients are slow

### 6. Enhanced Metrics & Monitoring
- **Prometheus integration**: Export metrics in Prometheus format
- **Per-topic metrics**: Detailed metrics per topic
- **Latency tracking**: P50, P95, P99 latencies
- **Alert thresholds**: Configurable alerts

### 7. Event Schemas
- **Schema registry**: Define and validate event schemas
- **Schema evolution**: Version management
- **Auto-validation**: Reject invalid events

### 8. Multi-Tenancy
- **Tenant isolation**: Separate namespaces
- **Per-tenant quotas**: Resource limits
- **Tenant-level auth**: Authentication per tenant

### 9. Event TTL (Time-To-Live)
- **Auto-expiration**: Expire old events
- **Per-topic TTL**: Different TTLs per topic
- **Delivery deadline**: Stop delivery after deadline

### 10. Enhanced Delivery Options
- **Delivery modes**: At-least-once, at-most-once, exactly-once
- **Ordered delivery**: Maintain event order
- **Batched webhooks**: Deliver multiple events per webhook

### 11. Admin UI
- **Web dashboard**: Visual management interface
- **Real-time monitoring**: Live delivery status
- **Event browser**: Browse and search events

### 12. Additional Backends
- **Redis Streams**: Lightweight alternative
- **NATS**: Cloud-native messaging
- **RabbitMQ**: Enterprise message broker
- **Google Cloud Pub/Sub**: GCP integration

### 13. Client SDK
- **Official SDKs**: Python, Node.js, Go, Java
- **Webhook framework integration**: Express, FastAPI, etc.
- **Auto-retry logic**: Built into SDKs

### 14. Event Transformation
- **Payload transformation**: Transform before delivery
- **Enrichment**: Add data from external sources
- **Filtering**: Transform only matching events

### 15. Audit Logging
- **Full audit trail**: Track all operations
- **Compliance**: SOC2, GDPR compliance
- **Tamper-proof**: Cryptographic verification
