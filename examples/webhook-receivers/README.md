# PMP-MQ Webhook Receiver Examples

This directory contains example webhook receivers in different programming languages.

## Available Examples

### 1. Python (FastAPI)
**File**: `python_fastapi.py`

```bash
# Install dependencies
pip install fastapi uvicorn pydantic

# Run
uvicorn python_fastapi:app --host 0.0.0.0 --port 8000
```

**Features**:
- ✅ FastAPI-based async webhook receiver
- ✅ Type-safe with Pydantic models
- ✅ Idempotent event processing
- ✅ Event storage and listing
- ✅ Health check endpoint
- ✅ Structured logging

### 2. Node.js (Express)
**File**: `nodejs_express.js`

```bash
# Install dependencies
npm install express body-parser

# Run
node nodejs_express.js
```

**Features**:
- ✅ Express-based webhook receiver
- ✅ Idempotent event processing
- ✅ Event storage and listing
- ✅ Health check endpoint
- ✅ Console logging

## Registering Your Webhook Receiver

After starting your webhook receiver, register it with PMP-MQ:

```bash
curl -X POST http://localhost:8080/api/v1/clients \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-webhook-receiver",
    "webhook_url": "http://your-host:port/webhook",
    "subscription_name": "your-subscription-name",
    "auth_headers": {
      "Authorization": "Bearer your-token-here"
    }
  }'
```

### Docker / Docker Compose

If PMP-MQ is running in Docker and your webhook receiver is on the host:

```bash
# Use host.docker.internal instead of localhost
"webhook_url": "http://host.docker.internal:8000/webhook"
```

If both are in Docker Compose:

```bash
# Use the service name
"webhook_url": "http://webhook-receiver:8000/webhook"
```

## Testing

### 1. Publish a Test Event

```bash
curl -X POST http://localhost:8080/api/v1/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "test.events",
    "event_type": "user.created",
    "payload": {
      "user_id": "123",
      "email": "test@example.com",
      "name": "Test User"
    },
    "metadata": {
      "source": "test-script"
    }
  }'
```

### 2. Check Received Events

```bash
# Python/FastAPI
curl http://localhost:8000/events

# Node.js/Express
curl http://localhost:3000/events
```

### 3. Health Check

```bash
# Python/FastAPI
curl http://localhost:8000/health

# Node.js/Express
curl http://localhost:3000/health
```

## Best Practices

### 1. Idempotency
Always track processed event IDs to handle duplicate deliveries:

```python
# Python
if event.id not in processed_events:
    process_event(event)
    processed_events.add(event.id)
```

```javascript
// Node.js
if (!processedEventIds.has(event.id)) {
    processEvent(event);
    processedEventIds.add(event.id);
}
```

### 2. Fast Response
Return HTTP 200 quickly and process asynchronously:

```python
# Python
@app.post("/webhook")
async def webhook(event: Event, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_event, event)
    return {"status": "accepted"}
```

```javascript
// Node.js
app.post('/webhook', (req, res) => {
    res.status(200).json({ status: 'accepted' });
    // Process asynchronously
    processEventAsync(req.body);
});
```

### 3. Error Handling
Return 5xx for retryable errors, 4xx for permanent failures:

```python
# Python
try:
    process_event(event)
    return {"status": "success"}
except RetryableError:
    raise HTTPException(status_code=500)  # Will retry
except PermanentError:
    raise HTTPException(status_code=400)  # Won't retry
```

### 4. Authentication
Verify webhook signatures or tokens:

```python
# Python
@app.post("/webhook")
async def webhook(event: Event, authorization: str = Header(None)):
    if authorization != f"Bearer {SECRET_TOKEN}":
        raise HTTPException(status_code=401)
    # Process event
```

### 5. Logging
Log all events for debugging:

```python
# Python
logger.info(f"Received event {event.id} from topic {event.topic}")
logger.debug(f"Payload: {event.payload}")
```

## Production Considerations

### Security
- ✅ Use HTTPS in production
- ✅ Verify webhook signatures
- ✅ Implement rate limiting
- ✅ Validate event structure

### Reliability
- ✅ Store events in database (not in-memory)
- ✅ Handle database failures gracefully
- ✅ Implement circuit breakers
- ✅ Monitor processing time

### Scalability
- ✅ Use async processing
- ✅ Process in background workers
- ✅ Horizontal scaling ready
- ✅ Database connection pooling

## Advanced Example: Database Storage

```python
# Python with SQLAlchemy
from sqlalchemy.orm import Session

@app.post("/webhook")
async def webhook(event: Event, db: Session = Depends(get_db)):
    # Check if already processed
    existing = db.query(ProcessedEvent).filter_by(event_id=event.id).first()
    if existing:
        return {"status": "already_processed"}

    # Store and process
    db.add(ProcessedEvent(event_id=event.id, payload=event.payload))
    db.commit()

    # Trigger async processing
    await process_event_async(event)

    return {"status": "success"}
```

## Troubleshooting

### Events not being delivered?
1. Check client is active: `GET /api/v1/clients`
2. Check subscription exists: `GET /api/v1/subscriptions`
3. Check delivery attempts: `GET /api/v1/events/{event_id}/attempts`
4. Check webhook receiver logs

### Getting 500 errors?
- Check webhook receiver is running
- Verify URL is accessible from PMP-MQ
- Check firewall/network settings
- Look at webhook receiver logs

### Duplicate events?
- This is expected! Implement idempotent processing
- Check event.id before processing
- Use database unique constraints

## More Examples

See `EXAMPLES.md` in the project root for more complete scenarios and integration patterns.
