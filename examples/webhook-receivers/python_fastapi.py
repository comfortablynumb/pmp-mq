"""
PMP-MQ Webhook Receiver Example - FastAPI (Python)

This example shows how to receive webhook events from PMP-MQ using FastAPI.

Install dependencies:
    pip install fastapi uvicorn pydantic

Run:
    uvicorn python_fastapi:app --host 0.0.0.0 --port 8000

Then register with PMP-MQ:
    curl -X POST http://localhost:8080/api/v1/clients \
      -H "Content-Type: application/json" \
      -d '{
        "name": "python-receiver",
        "webhook_url": "http://host.docker.internal:8000/webhook",
        "subscription_name": "your-subscription-name"
      }'
"""

from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="PMP-MQ Webhook Receiver")

# Event model matching PMP-MQ event structure
class Event(BaseModel):
    id: str
    topic: str
    event_type: Optional[str] = None
    payload: Any
    metadata: Any
    created_at: datetime

# Store received events (in-memory for demo)
events_received = []

@app.post("/webhook")
async def receive_webhook(event: Event):
    """
    Receive webhook from PMP-MQ

    Returns:
        - 200: Event processed successfully
        - 500: Error processing event (will trigger retry)
    """
    logger.info(f"📨 Received event: {event.id} from topic: {event.topic}")
    logger.info(f"   Event type: {event.event_type}")
    logger.info(f"   Payload: {event.payload}")

    try:
        # Process the event based on event_type
        if event.event_type == "user.created":
            handle_user_created(event)
        elif event.event_type == "order.placed":
            handle_order_placed(event)
        else:
            logger.warning(f"Unknown event type: {event.event_type}")

        # Store event (idempotent - check if already processed)
        event_ids = [e.id for e in events_received]
        if event.id not in event_ids:
            events_received.append(event)
            logger.info(f"✅ Event {event.id} processed and stored")
        else:
            logger.info(f"⚠️  Event {event.id} already processed (idempotent)")

        return {"status": "success", "event_id": event.id}

    except Exception as e:
        logger.error(f"❌ Error processing event {event.id}: {e}")
        # Return 500 to trigger retry
        raise HTTPException(status_code=500, detail=str(e))

def handle_user_created(event: Event):
    """Handle user.created events"""
    user_id = event.payload.get("user_id")
    email = event.payload.get("email")
    logger.info(f"👤 New user created: {user_id} ({email})")
    # Add your business logic here

def handle_order_placed(event: Event):
    """Handle order.placed events"""
    order_id = event.payload.get("order_id")
    total = event.payload.get("total")
    logger.info(f"🛒 New order placed: {order_id} - Total: ${total}")
    # Add your business logic here

@app.get("/events")
async def list_events():
    """List all received events"""
    return {
        "total": len(events_received),
        "events": events_received
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "events_processed": len(events_received)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
