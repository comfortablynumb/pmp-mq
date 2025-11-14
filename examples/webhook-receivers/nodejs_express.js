/**
 * PMP-MQ Webhook Receiver Example - Express (Node.js)
 *
 * This example shows how to receive webhook events from PMP-MQ using Express.
 *
 * Install dependencies:
 *     npm install express body-parser
 *
 * Run:
 *     node nodejs_express.js
 *
 * Then register with PMP-MQ:
 *     curl -X POST http://localhost:8080/api/v1/clients \
 *       -H "Content-Type: application/json" \
 *       -d '{
 *         "name": "nodejs-receiver",
 *         "webhook_url": "http://host.docker.internal:3000/webhook",
 *         "subscription_name": "your-subscription-name"
 *       }'
 */

const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const PORT = 3000;

// Middleware
app.use(bodyParser.json());

// Store received events (in-memory for demo)
const eventsReceived = [];
const processedEventIds = new Set();

// Webhook endpoint
app.post('/webhook', async (req, res) => {
  const event = req.body;

  console.log(`📨 Received event: ${event.id} from topic: ${event.topic}`);
  console.log(`   Event type: ${event.event_type}`);
  console.log(`   Payload:`, event.payload);

  try {
    // Process the event based on event_type
    switch (event.event_type) {
      case 'user.created':
        handleUserCreated(event);
        break;
      case 'order.placed':
        handleOrderPlaced(event);
        break;
      default:
        console.warn(`Unknown event type: ${event.event_type}`);
    }

    // Idempotent processing - check if already processed
    if (processedEventIds.has(event.id)) {
      console.log(`⚠️  Event ${event.id} already processed (idempotent)`);
    } else {
      processedEventIds.add(event.id);
      eventsReceived.push({
        ...event,
        processed_at: new Date().toISOString()
      });
      console.log(`✅ Event ${event.id} processed and stored`);
    }

    // Return 200 to acknowledge receipt
    res.status(200).json({
      status: 'success',
      event_id: event.id
    });

  } catch (error) {
    console.error(`❌ Error processing event ${event.id}:`, error);
    // Return 500 to trigger retry
    res.status(500).json({
      status: 'error',
      error: error.message
    });
  }
});

function handleUserCreated(event) {
  const { user_id, email } = event.payload;
  console.log(`👤 New user created: ${user_id} (${email})`);
  // Add your business logic here
}

function handleOrderPlaced(event) {
  const { order_id, total } = event.payload;
  console.log(`🛒 New order placed: ${order_id} - Total: $${total}`);
  // Add your business logic here
}

// List events endpoint
app.get('/events', (req, res) => {
  res.json({
    total: eventsReceived.length,
    events: eventsReceived
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    events_processed: eventsReceived.length
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Webhook receiver listening on http://localhost:${PORT}`);
  console.log(`📡 Webhook endpoint: http://localhost:${PORT}/webhook`);
});
