# PMP-MQ Admin Dashboard

A simple, single-page admin dashboard for monitoring and managing your PMP-MQ instance.

## Features

- **Metrics Overview**: Real-time system metrics with auto-refresh
- **Topics Management**: View all topics and their configurations
- **Subscriptions**: Monitor active subscriptions and their filters
- **Clients**: View registered webhook clients and their status
- **Dead Letter Queue**: Inspect, retry, and delete failed deliveries
- **Event Publishing**: Test event publishing with custom payloads

## Quick Start

### Option 1: Serve with Python

```bash
cd dashboard
python3 -m http.server 8081
```

Then open http://localhost:8081 in your browser.

### Option 2: Serve with Node.js

```bash
cd dashboard
npx serve -p 8081
```

Then open http://localhost:8081 in your browser.

### Option 3: Serve with any web server

Configure your web server (nginx, Apache, Caddy, etc.) to serve the `dashboard` directory.

## Configuration

1. Open the dashboard in your browser
2. Update the **API Base URL** field to point to your PMP-MQ instance
   - Default: `http://localhost:8080/api/v1`
   - For docker: `http://localhost:8080/api/v1` or `http://host.docker.internal:8080/api/v1`
   - For production: Your production PMP-MQ URL

## Usage

### Viewing Metrics

The **Metrics** tab shows real-time statistics:
- Total topics, subscriptions, and clients
- Active vs inactive clients
- Pending and failed deliveries
- Dead letter queue count

Metrics auto-refresh every 5 seconds.

### Managing Topics

The **Topics** tab displays:
- Topic names and descriptions
- Event storage configuration
- Creation timestamps

### Monitoring Subscriptions

The **Subscriptions** tab shows:
- Subscription names
- Associated topics
- Event filters (if configured)

### Viewing Clients

The **Clients** tab lists:
- Client names and webhook URLs
- Subscription assignments
- Active/inactive status

### Dead Letter Queue Management

The **Dead Letter Queue** tab allows you to:
- View failed deliveries
- See error messages and attempt counts
- Retry individual events
- Delete individual events
- Bulk retry up to 100 events
- Bulk delete up to 100 events

### Publishing Test Events

The **Publish Event** tab lets you:
1. Select a topic
2. Specify an event type (optional)
3. Enter a JSON payload
4. Add metadata (optional)
5. Publish the event

Example payload:
```json
{
  "user_id": "123",
  "action": "signup",
  "email": "user@example.com"
}
```

## CORS Configuration

If your PMP-MQ instance is on a different domain, you may need to enable CORS. The server should be configured to accept requests from your dashboard's origin.

## Security Considerations

### For Development
- The dashboard connects directly to the PMP-MQ API
- Suitable for local development and testing

### For Production
- **Do not expose this dashboard publicly without authentication**
- Use a reverse proxy (nginx, Traefik) with authentication
- Implement IP whitelisting
- Use HTTPS for all connections
- Consider adding an auth layer (Basic Auth, OAuth, etc.)

Example nginx configuration with Basic Auth:
```nginx
server {
    listen 80;
    server_name admin.yourdomain.com;

    location / {
        auth_basic "PMP-MQ Admin";
        auth_basic_user_file /etc/nginx/.htpasswd;
        root /path/to/dashboard;
        index index.html;
    }

    location /api/ {
        proxy_pass http://pmp-mq-server:8080/api/;
    }
}
```

## Browser Compatibility

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- No IE support

## Customization

The dashboard is built with vanilla HTML, CSS, and JavaScript. You can easily customize:

- **Styling**: Edit the `<style>` block in `index.html`
- **Functionality**: Modify `dashboard.js`
- **Layout**: Update the HTML structure in `index.html`

## Troubleshooting

### "Failed to load metrics" error
- Check that the API Base URL is correct
- Verify PMP-MQ is running and accessible
- Check browser console for CORS errors
- Verify network connectivity

### Metrics not updating
- Check browser console for JavaScript errors
- Ensure auto-refresh isn't disabled
- Try manual refresh with the "Refresh" button

### Publishing events fails
- Verify JSON payload is valid
- Check that the topic exists
- Ensure PMP-MQ is running and accessible

## Contributing

This is a simple dashboard meant for basic monitoring and testing. For more advanced features, consider:
- Building a full React/Vue application
- Adding authentication/authorization
- Implementing advanced filtering and search
- Adding charts and graphs
- Real-time WebSocket updates
