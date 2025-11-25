# PMP-MQ Usage Examples

This document provides detailed examples of using PMP-MQ for common scenarios.

## Example 1: User Activity Tracking

Track user activities and send them to multiple services.

### Setup

1. Create a topic for user events:
```bash
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "user.activity",
    "description": "User activity events"
  }'
```

2. Create a subscription:
```bash
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "analytics-sub",
    "topic_name": "user.activity",
    "config": {
      "max_delivery_attempts": 3
    }
  }'
```

3. Register your analytics service:
```bash
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "analytics-service",
    "webhook_url": "https://analytics.example.com/webhooks/user-events",
    "subscription_name": "analytics-sub"
  }'
```

4. Publish user events:
```bash
curl -X POST http://localhost:8080/api/v1/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.activity",
    "event_type": "user.login",
    "payload": {
      "user_id": "user_123",
      "timestamp": "2025-11-14T10:30:00Z",
      "ip_address": "192.168.1.1",
      "user_agent": "Mozilla/5.0..."
    }
  }'
```

## Example 2: Order Processing Pipeline

Process orders through multiple stages using events.

### Topics and Subscriptions

```bash
# Create order events topic
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders.events"}'

# Create subscriptions for different services
# Payment service
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "payment-processor",
    "topic_name": "orders.events"
  }'

# Inventory service
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "inventory-manager",
    "topic_name": "orders.events"
  }'

# Notification service
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "notifications",
    "topic_name": "orders.events"
  }'
```

### Register Clients

```bash
# Payment service
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "payment-service",
    "webhook_url": "http://payment-service:3000/webhooks/orders",
    "subscription_name": "payment-processor"
  }'

# Inventory service
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "inventory-service",
    "webhook_url": "http://inventory-service:3001/webhooks/orders",
    "subscription_name": "inventory-manager"
  }'

# Notification service
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "notification-service",
    "webhook_url": "http://notification-service:3002/webhooks/orders",
    "subscription_name": "notifications"
  }'
```

### Publish Order Events

```bash
# Order created
curl -X POST http://localhost:8080/api/v1/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders.events",
    "event_type": "order.created",
    "payload": {
      "order_id": "ord_12345",
      "customer_id": "cust_789",
      "items": [
        {"product_id": "prod_1", "quantity": 2, "price": 29.99}
      ],
      "total": 59.98,
      "status": "pending"
    }
  }'
```

## Example 3: Webhook Receiver (Node.js)

Example webhook receiver service:

```javascript
const express = require('express');
const app = express();

app.use(express.json());

// Webhook endpoint
app.post('/webhooks/events', async (req, res) => {
  const event = req.body;

  console.log('Received event:', {
    id: event.id,
    topic: event.topic,
    event_type: event.event_type,
    created_at: event.created_at
  });

  try {
    // Process the event
    await processEvent(event);

    // Return 200 to acknowledge receipt
    res.status(200).json({ received: true });
  } catch (error) {
    console.error('Error processing event:', error);

    // Return error status to trigger retry
    res.status(500).json({ error: error.message });
  }
});

async function processEvent(event) {
  // Your business logic here
  switch (event.event_type) {
    case 'user.created':
      await handleUserCreated(event.payload);
      break;
    case 'order.created':
      await handleOrderCreated(event.payload);
      break;
    default:
      console.log('Unknown event type:', event.event_type);
  }
}

app.listen(3000, () => {
  console.log('Webhook receiver listening on port 3000');
});
```

## Example 4: Webhook Receiver (Python/FastAPI)

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
import logging

app = FastAPI()
logging.basicConfig(level=logging.INFO)

class Event(BaseModel):
    id: str
    topic: str
    event_type: Optional[str]
    payload: Any
    metadata: Any
    created_at: str

@app.post("/webhooks/events")
async def receive_event(event: Event):
    logging.info(f"Received event: {event.id} - {event.event_type}")

    try:
        # Process the event
        await process_event(event)

        return {"received": True}
    except Exception as e:
        logging.error(f"Error processing event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def process_event(event: Event):
    # Your business logic here
    if event.event_type == "user.created":
        await handle_user_created(event.payload)
    elif event.event_type == "order.created":
        await handle_order_created(event.payload)
    else:
        logging.warning(f"Unknown event type: {event.event_type}")

async def handle_user_created(payload):
    # Process user creation
    pass

async def handle_order_created(payload):
    # Process order creation
    pass
```

## Example 5: Testing Webhook Delivery Locally

Use a tool like [webhook.site](https://webhook.site) or run a local test server:

### Using Python for quick testing:

```python
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        event = json.loads(body)

        print(f"\n=== Received Event ===")
        print(json.dumps(event, indent=2))
        print(f"=====================\n")

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status": "received"}')

httpd = HTTPServer(('localhost', 8888), WebhookHandler)
print("Webhook test server running on http://localhost:8888")
httpd.serve_forever()
```

Then register this endpoint:
```bash
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-client",
    "webhook_url": "http://localhost:8888",
    "subscription_name": "your-subscription"
  }'
```

## Example 6: Managing Client Status

Temporarily disable a client without deleting it:

```bash
# Disable client
curl -X PUT http://localhost:8080/api/v1/clients/{client-id}/status \
  -H "Content-Type: application/json" \
  -d '{"active": false}'

# Re-enable client
curl -X PUT http://localhost:8080/api/v1/clients/{client-id}/status \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

## Example 7: Monitoring Delivery Status

Check delivery attempts for an event:

```bash
# Get event ID from publish response
EVENT_ID="550e8400-e29b-41d4-a716-446655440000"

# Check delivery attempts
curl http://localhost:8080/api/v1/events/$EVENT_ID/attempts
```

Response:
```json
[
  {
    "id": "attempt-uuid",
    "event_id": "event-uuid",
    "client_id": "client-uuid",
    "attempt_number": 1,
    "status": "Failed",
    "attempted_at": "2025-11-14T10:30:00Z",
    "completed_at": "2025-11-14T10:30:05Z",
    "error_message": "HTTP status: 500",
    "http_status_code": 500
  }
]
```

## Example 8: Authentication Headers

Register a client with custom authentication:

```bash
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "secure-service",
    "webhook_url": "https://api.example.com/webhooks",
    "subscription_name": "my-subscription",
    "auth_headers": {
      "Authorization": "Bearer eyJhbGciOiJIUzI1NiIs...",
      "X-API-Key": "your-api-key",
      "X-Custom-Header": "custom-value"
    }
  }'
```

## Best Practices

1. **Idempotency**: Always process events idempotently since they may be delivered multiple times
2. **Fast Response**: Respond to webhooks quickly (< 5s). Do heavy processing asynchronously
3. **Error Handling**: Return 2xx for success, 5xx for retryable errors, 4xx for permanent failures
4. **Monitoring**: Regularly check delivery attempts for failed events
5. **Cleanup**: Remove or disable inactive clients to avoid unnecessary delivery attempts
