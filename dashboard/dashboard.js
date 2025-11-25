// Get API base URL
function getApiUrl() {
    return document.getElementById('api-url').value;
}

// Show tab
function showTab(tabName) {
    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });

    // Remove active class from all tabs
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });

    // Show selected tab
    document.getElementById(`${tabName}-tab`).classList.add('active');

    // Activate button
    event.target.classList.add('active');

    // Refresh data for the tab
    switch(tabName) {
        case 'metrics':
            refreshMetrics();
            break;
        case 'topics':
            refreshTopics();
            break;
        case 'subscriptions':
            refreshSubscriptions();
            break;
        case 'clients':
            refreshClients();
            break;
        case 'dlq':
            refreshDLQ();
            break;
    }
}

// Fetch wrapper with error handling
async function apiFetch(endpoint, options = {}) {
    try {
        const url = `${getApiUrl()}${endpoint}`;
        const response = await fetch(url, options);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// Refresh Metrics
async function refreshMetrics() {
    const container = document.getElementById('metrics-container');
    container.innerHTML = '<div class="loading">Loading metrics...</div>';

    try {
        const metrics = await apiFetch('/metrics');

        container.innerHTML = `
            <div class="metric-card">
                <h3>Topics</h3>
                <div class="value">${metrics.topics_count}</div>
            </div>
            <div class="metric-card">
                <h3>Subscriptions</h3>
                <div class="value">${metrics.subscriptions_count}</div>
            </div>
            <div class="metric-card">
                <h3>Clients</h3>
                <div class="value">${metrics.clients_count}</div>
            </div>
            <div class="metric-card">
                <h3>Active Clients</h3>
                <div class="value">${metrics.active_clients_count}</div>
            </div>
            <div class="metric-card">
                <h3>Pending Deliveries</h3>
                <div class="value">${metrics.pending_deliveries}</div>
            </div>
            <div class="metric-card">
                <h3>Failed Deliveries</h3>
                <div class="value">${metrics.failed_deliveries}</div>
            </div>
            <div class="metric-card">
                <h3>Dead Letter Queue</h3>
                <div class="value">${metrics.dead_letter_count}</div>
            </div>
        `;
    } catch (error) {
        container.innerHTML = `<div class="error">Failed to load metrics: ${error.message}</div>`;
    }
}

// Refresh Topics
async function refreshTopics() {
    const container = document.getElementById('topics-container');
    container.innerHTML = '<div class="loading">Loading topics...</div>';

    try {
        const topics = await apiFetch('/topics');

        if (topics.length === 0) {
            container.innerHTML = '<div class="empty-state">No topics found</div>';
            return;
        }

        const table = `
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Description</th>
                        <th>Store Events</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>
                    ${topics.map(topic => `
                        <tr>
                            <td><strong>${topic.name}</strong></td>
                            <td>${topic.description || '-'}</td>
                            <td>${topic.config.store_events ? 'Yes' : 'No'}</td>
                            <td>${new Date(topic.created_at).toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        container.innerHTML = table;
    } catch (error) {
        container.innerHTML = `<div class="error">Failed to load topics: ${error.message}</div>`;
    }
}

// Refresh Subscriptions
async function refreshSubscriptions() {
    const container = document.getElementById('subscriptions-container');
    container.innerHTML = '<div class="loading">Loading subscriptions...</div>';

    try {
        const subscriptions = await apiFetch('/subscriptions');

        if (subscriptions.length === 0) {
            container.innerHTML = '<div class="empty-state">No subscriptions found</div>';
            return;
        }

        const table = `
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Topic</th>
                        <th>Filter</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>
                    ${subscriptions.map(sub => `
                        <tr>
                            <td><strong>${sub.name}</strong></td>
                            <td>${sub.topic_name}</td>
                            <td><code>${sub.filter || 'None'}</code></td>
                            <td>${new Date(sub.created_at).toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        container.innerHTML = table;
    } catch (error) {
        container.innerHTML = `<div class="error">Failed to load subscriptions: ${error.message}</div>`;
    }
}

// Refresh Clients
async function refreshClients() {
    const container = document.getElementById('clients-container');
    container.innerHTML = '<div class="loading">Loading clients...</div>';

    try {
        const clients = await apiFetch('/clients');

        if (clients.length === 0) {
            container.innerHTML = '<div class="empty-state">No clients found</div>';
            return;
        }

        const table = `
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Subscription</th>
                        <th>Webhook URL</th>
                        <th>Status</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>
                    ${clients.map(client => `
                        <tr>
                            <td><strong>${client.name}</strong></td>
                            <td>${client.subscription_name}</td>
                            <td><code>${client.webhook_url}</code></td>
                            <td>
                                <span class="status-badge ${client.active ? 'status-active' : 'status-inactive'}">
                                    ${client.active ? 'Active' : 'Inactive'}
                                </span>
                            </td>
                            <td>${new Date(client.created_at).toLocaleString()}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        container.innerHTML = table;
    } catch (error) {
        container.innerHTML = `<div class="error">Failed to load clients: ${error.message}</div>`;
    }
}

// Refresh DLQ
async function refreshDLQ() {
    const container = document.getElementById('dlq-container');
    container.innerHTML = '<div class="loading">Loading DLQ events...</div>';

    try {
        const response = await apiFetch('/dlq/events?limit=100');

        if (response.events.length === 0) {
            container.innerHTML = '<div class="empty-state">No events in dead letter queue</div>';
            return;
        }

        const table = `
            <p><strong>Total DLQ Events:</strong> ${response.total}</p>
            <table>
                <thead>
                    <tr>
                        <th>Event ID</th>
                        <th>Topic</th>
                        <th>Client</th>
                        <th>Attempts</th>
                        <th>Last Error</th>
                        <th>Created At</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    ${response.events.map(item => `
                        <tr>
                            <td><code>${item.event.id}</code></td>
                            <td>${item.event.topic}</td>
                            <td>${item.client.name}</td>
                            <td>${item.total_attempts}</td>
                            <td>${item.last_attempt.error_message || '-'}</td>
                            <td>${new Date(item.event.created_at).toLocaleString()}</td>
                            <td>
                                <div class="btn-group">
                                    <button class="btn btn-success" onclick="retryDLQEvent('${item.event.id}', '${item.client.id}')">Retry</button>
                                    <button class="btn btn-danger" onclick="deleteDLQEvent('${item.event.id}', '${item.client.id}')">Delete</button>
                                </div>
                            </td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        container.innerHTML = table;
    } catch (error) {
        container.innerHTML = `<div class="error">Failed to load DLQ events: ${error.message}</div>`;
    }
}

// Retry single DLQ event
async function retryDLQEvent(eventId, clientId) {
    if (!confirm('Retry this event?')) return;

    try {
        await apiFetch(`/dlq/events/${eventId}/${clientId}/retry`, {
            method: 'POST'
        });

        alert('Event moved back to pending queue');
        refreshDLQ();
    } catch (error) {
        alert(`Failed to retry event: ${error.message}`);
    }
}

// Delete single DLQ event
async function deleteDLQEvent(eventId, clientId) {
    if (!confirm('Delete this event? This cannot be undone.')) return;

    try {
        await apiFetch(`/dlq/events/${eventId}/${clientId}`, {
            method: 'DELETE'
        });

        alert('Event deleted');
        refreshDLQ();
    } catch (error) {
        alert(`Failed to delete event: ${error.message}`);
    }
}

// Bulk retry DLQ events
async function bulkRetryDLQ() {
    if (!confirm('Retry up to 100 events from DLQ?')) return;

    try {
        const result = await apiFetch('/dlq/bulk-retry', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ limit: 100 })
        });

        alert(`Moved ${result.affected_count} events back to pending queue`);
        refreshDLQ();
    } catch (error) {
        alert(`Failed to bulk retry: ${error.message}`);
    }
}

// Bulk delete DLQ events
async function bulkDeleteDLQ() {
    if (!confirm('Delete up to 100 events from DLQ? This cannot be undone.')) return;

    try {
        const result = await apiFetch('/dlq/bulk-delete', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ limit: 100 })
        });

        alert(`Deleted ${result.affected_count} events from DLQ`);
        refreshDLQ();
    } catch (error) {
        alert(`Failed to bulk delete: ${error.message}`);
    }
}

// Publish Event
async function publishEvent(event) {
    event.preventDefault();

    const resultDiv = document.getElementById('publish-result');
    resultDiv.innerHTML = '';

    try {
        const payload = JSON.parse(document.getElementById('payload').value);
        const metadata = JSON.parse(document.getElementById('metadata').value);

        const data = {
            topic: document.getElementById('topic').value,
            payload: payload,
            metadata: metadata
        };

        const eventType = document.getElementById('event-type').value;
        if (eventType) {
            data.event_type = eventType;
        }

        const result = await apiFetch('/events/publish', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });

        resultDiv.innerHTML = `
            <div class="success">
                Event published successfully!
                <div class="json-preview">${JSON.stringify(result, null, 2)}</div>
            </div>
        `;

        // Clear form
        document.getElementById('topic').value = '';
        document.getElementById('event-type').value = '';
        document.getElementById('payload').value = '{"message": "Hello World"}';
        document.getElementById('metadata').value = '{}';
    } catch (error) {
        resultDiv.innerHTML = `<div class="error">Failed to publish event: ${error.message}</div>`;
    }
}

// Auto-refresh metrics every 5 seconds
setInterval(() => {
    const metricsTab = document.getElementById('metrics-tab');
    if (metricsTab.classList.contains('active')) {
        refreshMetrics();
    }
}, 5000);

// Initial load
refreshMetrics();
