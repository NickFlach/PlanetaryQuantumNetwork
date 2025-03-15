"""
Enhanced Dashboard for Distributed OT Network

This script provides a robust web dashboard for visualizing the distributed OT network
with better error handling and guaranteed port access.
"""

import os
import sys
import json
import logging
import asyncio
import random
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# In-memory document storage
DEFAULT_DOCUMENT = "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transformation.\nYou can edit this text and see how changes are synchronized across the network."
document = DEFAULT_DOCUMENT
document_history = []

# Network simulation data
planets = ["earth", "mars", "jupiter"]
nodes = [
    {"node_id": "earth-node-1", "planet_id": "earth", "online": True, "last_seen": time.time()},
    {"node_id": "earth-node-2", "planet_id": "earth", "online": True, "last_seen": time.time()},
    {"node_id": "mars-node-1", "planet_id": "mars", "online": True, "last_seen": time.time()},
    {"node_id": "mars-node-2", "planet_id": "mars", "online": True, "last_seen": time.time()},
    {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True, "last_seen": time.time()}
]
latencies = [
    {"source": "earth", "target": "mars", "value": 225.0},
    {"source": "mars", "target": "earth", "value": 225.0},
    {"source": "earth", "target": "jupiter", "value": 750.0},
    {"source": "jupiter", "target": "earth", "value": 750.0},
    {"source": "mars", "target": "jupiter", "value": 550.0},
    {"source": "jupiter", "target": "mars", "value": 550.0}
]

# Event simulation data
events = []
operation_count = 0
network_partition_events = []

# Operating metrics
metrics = {
    "started_at": time.time(),
    "operations_processed": 0,
    "sync_events": 0,
    "network_partitions": 0,
    "average_latency": 0,
    "convergence_time": 0
}

def apply_operation(op_type, position, content):
    """Apply an operation to the document."""
    global document, operation_count, metrics
    
    try:
        if op_type == 'insert':
            document = document[:position] + content + document[position:]
            document_history.append({
                'type': 'insert',
                'position': position,
                'content': content,
                'timestamp': time.time()
            })
            
            # Log the event
            events.append({
                'event_type': 'operation',
                'operation_type': 'insert',
                'source_node': random.choice([node['node_id'] for node in nodes if node['online']]),
                'content_length': len(content),
                'timestamp': time.time()
            })
            
        elif op_type == 'delete':
            if position < len(document):
                length = min(len(content) if content else 1, len(document) - position)
                deleted = document[position:position+length]
                document = document[:position] + document[position+length:]
                document_history.append({
                    'type': 'delete',
                    'position': position,
                    'content': deleted,
                    'timestamp': time.time()
                })
                
                # Log the event
                events.append({
                    'event_type': 'operation',
                    'operation_type': 'delete',
                    'source_node': random.choice([node['node_id'] for node in nodes if node['online']]),
                    'content_length': len(deleted),
                    'timestamp': time.time()
                })
        
        # Update metrics
        operation_count += 1
        metrics['operations_processed'] += 1
        
        # Simulate convergence time
        metrics['convergence_time'] = random.uniform(50, 500) 
        
        return document
    except Exception as e:
        logger.error(f"Error applying operation: {e}")
        return document

def get_html_template():
    """Return the HTML template for the dashboard."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Distributed OT Network Dashboard</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f7fa;
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
        }
        h1, h2, h3 {
            color: #2c3e50;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }
        .stats-container {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background-color: #fff;
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
            border-left: 4px solid #3498db;
        }
        .stat-card h3 {
            margin-top: 0;
            font-size: 16px;
            color: #7f8c8d;
        }
        .stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #2c3e50;
        }
        .planets-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .planet-card {
            background-color: #fff;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.05);
        }
        .planet-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        .planet-name {
            font-size: 18px;
            font-weight: bold;
            color: #2c3e50;
        }
        .planet-status {
            font-size: 14px;
            padding: 4px 8px;
            border-radius: 4px;
            background-color: #27ae60;
            color: white;
        }
        .nodes-list {
            margin-top: 15px;
        }
        .node-item {
            padding: 10px;
            margin-bottom: 8px;
            background-color: #f8f9fa;
            border-radius: 4px;
            display: flex;
            justify-content: space-between;
        }
        .node-id {
            font-family: monospace;
            color: #34495e;
        }
        .node-connection {
            font-size: 14px;
            color: #7f8c8d;
        }
        .node-connected {
            color: #27ae60;
        }
        .node-disconnected {
            color: #e74c3c;
        }
        .document-section {
            margin-top: 30px;
        }
        #document-content {
            width: 100%;
            height: 200px;
            font-family: monospace;
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #ddd;
            margin-bottom: 10px;
        }
        .operation-form {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .form-group {
            display: flex;
            flex-direction: column;
        }
        .form-group label {
            margin-bottom: 5px;
            font-size: 14px;
            color: #7f8c8d;
        }
        input, select {
            padding: 8px;
            border-radius: 4px;
            border: 1px solid #ddd;
        }
        button {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        button:hover {
            background-color: #2980b9;
        }
        .latency-matrix {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 30px;
        }
        .latency-matrix th, .latency-matrix td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        .latency-matrix th {
            background-color: #f8f9fa;
        }
        .events-section {
            margin-top: 30px;
        }
        .events-list {
            max-height: 300px;
            overflow-y: auto;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
        }
        .event-item {
            padding: 8px;
            border-bottom: 1px solid #eee;
            font-size: 14px;
        }
        .event-time {
            color: #7f8c8d;
            margin-right: 10px;
        }
        .event-operation {
            color: #3498db;
        }
        .event-sync {
            color: #27ae60;
        }
        .event-partition {
            color: #e74c3c;
        }
        .message {
            padding: 10px;
            border-radius: 4px;
            margin-bottom: 10px;
        }
        .message-success {
            background-color: #d4edda;
            color: #155724;
        }
        .message-error {
            background-color: #f8d7da;
            color: #721c24;
        }
        #uptime {
            font-family: monospace;
            font-size: 14px;
            color: #7f8c8d;
        }
        .refresh-button {
            background-color: #6c757d;
            margin-left: 10px;
        }
        .footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
            text-align: center;
            color: #7f8c8d;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1>Distributed OT Network Dashboard</h1>
                <div id="uptime">Uptime: calculating...</div>
            </div>
            <button id="refresh-btn" class="refresh-button">Refresh Data</button>
        </div>

        <div id="message-container"></div>

        <h2>Network Statistics</h2>
        <div class="stats-container">
            <div class="stat-card">
                <h3>Active Planets</h3>
                <div id="active-planets" class="stat-value">0</div>
            </div>
            <div class="stat-card">
                <h3>Active Nodes</h3>
                <div id="active-nodes" class="stat-value">0</div>
            </div>
            <div class="stat-card">
                <h3>Operations Processed</h3>
                <div id="operations-processed" class="stat-value">0</div>
            </div>
            <div class="stat-card">
                <h3>Avg. Latency (ms)</h3>
                <div id="avg-latency" class="stat-value">0</div>
            </div>
            <div class="stat-card">
                <h3>Avg. Convergence (ms)</h3>
                <div id="avg-convergence" class="stat-value">0</div>
            </div>
            <div class="stat-card">
                <h3>Network Partitions</h3>
                <div id="network-partitions" class="stat-value">0</div>
            </div>
        </div>

        <h2>Planets</h2>
        <div id="planets-container" class="planets-grid">
            <!-- Planets will be populated here -->
        </div>

        <h2>Latency Matrix (ms)</h2>
        <div id="latency-matrix-container">
            <!-- Latency matrix will be populated here -->
        </div>

        <div class="document-section">
            <h2>Document Editor</h2>
            <textarea id="document-content" readonly></textarea>
            <div class="operation-form">
                <div class="form-group">
                    <label for="operation-type">Operation Type</label>
                    <select id="operation-type">
                        <option value="insert">Insert</option>
                        <option value="delete">Delete</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="position">Position</label>
                    <input type="number" id="position" min="0" value="0">
                </div>
                <div class="form-group">
                    <label for="content">Content</label>
                    <input type="text" id="content" placeholder="Text to insert">
                </div>
                <div class="form-group">
                    <label>&nbsp;</label>
                    <button id="apply-btn">Apply Operation</button>
                </div>
            </div>
        </div>

        <div class="events-section">
            <h2>Recent Events</h2>
            <div id="events-container" class="events-list">
                <!-- Events will be populated here -->
            </div>
        </div>

        <div class="footer">
            <p>Distributed OT Network with Quantum Resistance &copy; 2025</p>
        </div>
    </div>

    <script>
        // Helper function to format time
        function formatTime(timestamp) {
            const date = new Date(timestamp * 1000);
            return date.toLocaleTimeString();
        }

        // Helper function to format duration
        function formatDuration(seconds) {
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            const secs = Math.floor(seconds % 60);
            
            return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
        }

        // Display a message
        function showMessage(text, type = 'success') {
            const messageContainer = document.getElementById('message-container');
            const messageElement = document.createElement('div');
            messageElement.className = `message message-${type}`;
            messageElement.textContent = text;
            
            messageContainer.appendChild(messageElement);
            
            // Remove after 5 seconds
            setTimeout(() => {
                messageContainer.removeChild(messageElement);
            }, 5000);
        }

        // Load document content
        function loadDocument() {
            fetch('/api/document')
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    document.getElementById('document-content').value = data.content;
                })
                .catch(error => {
                    console.error('Error loading document:', error);
                    showMessage('Error loading document content', 'error');
                });
        }

        // Load network status
        function loadNetworkStatus() {
            fetch('/api/network')
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    // Update statistics
                    const activePlanets = new Set(data.nodes.filter(node => node.online).map(node => node.planet_id)).size;
                    const activeNodes = data.nodes.filter(node => node.online).length;
                    
                    document.getElementById('active-planets').textContent = activePlanets;
                    document.getElementById('active-nodes').textContent = activeNodes;
                    
                    // Update planets
                    const planetsContainer = document.getElementById('planets-container');
                    planetsContainer.innerHTML = '';
                    
                    const planetMap = {};
                    data.nodes.forEach(node => {
                        if (!planetMap[node.planet_id]) {
                            planetMap[node.planet_id] = [];
                        }
                        planetMap[node.planet_id].push(node);
                    });
                    
                    Object.keys(planetMap).forEach(planetId => {
                        const planetNodes = planetMap[planetId];
                        const onlineNodes = planetNodes.filter(node => node.online).length;
                        
                        const planetCard = document.createElement('div');
                        planetCard.className = 'planet-card';
                        
                        const planetHeader = document.createElement('div');
                        planetHeader.className = 'planet-header';
                        
                        const planetName = document.createElement('div');
                        planetName.className = 'planet-name';
                        planetName.textContent = planetId.charAt(0).toUpperCase() + planetId.slice(1);
                        
                        const planetStatus = document.createElement('div');
                        planetStatus.className = 'planet-status';
                        planetStatus.textContent = `${onlineNodes}/${planetNodes.length} nodes online`;
                        
                        planetHeader.appendChild(planetName);
                        planetHeader.appendChild(planetStatus);
                        
                        const nodesList = document.createElement('div');
                        nodesList.className = 'nodes-list';
                        
                        planetNodes.forEach(node => {
                            const nodeItem = document.createElement('div');
                            nodeItem.className = 'node-item';
                            
                            const nodeId = document.createElement('div');
                            nodeId.className = 'node-id';
                            nodeId.textContent = node.node_id;
                            
                            const nodeConnection = document.createElement('div');
                            nodeConnection.className = `node-connection ${node.online ? 'node-connected' : 'node-disconnected'}`;
                            nodeConnection.textContent = node.online ? 'Connected' : 'Disconnected';
                            
                            nodeItem.appendChild(nodeId);
                            nodeItem.appendChild(nodeConnection);
                            nodesList.appendChild(nodeItem);
                        });
                        
                        planetCard.appendChild(planetHeader);
                        planetCard.appendChild(nodesList);
                        planetsContainer.appendChild(planetCard);
                    });
                })
                .catch(error => {
                    console.error('Error loading network status:', error);
                    showMessage('Error loading network status', 'error');
                });
        }

        // Load latency matrix
        function loadLatencyMatrix() {
            fetch('/api/latency')
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    const container = document.getElementById('latency-matrix-container');
                    container.innerHTML = '';
                    
                    const planets = data.planets;
                    const latencies = data.latencies;
                    
                    // Create a mapping for quick lookups
                    const latencyMap = {};
                    latencies.forEach(l => {
                        if (!latencyMap[l.source]) {
                            latencyMap[l.source] = {};
                        }
                        latencyMap[l.source][l.target] = l.value;
                    });
                    
                    // Create the table
                    const table = document.createElement('table');
                    table.className = 'latency-matrix';
                    
                    // Create header row
                    const headerRow = document.createElement('tr');
                    const cornerCell = document.createElement('th');
                    headerRow.appendChild(cornerCell);
                    
                    planets.forEach(planet => {
                        const th = document.createElement('th');
                        th.textContent = planet.charAt(0).toUpperCase() + planet.slice(1);
                        headerRow.appendChild(th);
                    });
                    
                    table.appendChild(headerRow);
                    
                    // Create data rows
                    planets.forEach(source => {
                        const row = document.createElement('tr');
                        
                        const th = document.createElement('th');
                        th.textContent = source.charAt(0).toUpperCase() + source.slice(1);
                        row.appendChild(th);
                        
                        planets.forEach(target => {
                            const td = document.createElement('td');
                            
                            if (source === target) {
                                td.textContent = '0';
                            } else {
                                const latency = latencyMap[source]?.[target] || '?';
                                td.textContent = latency;
                            }
                            
                            row.appendChild(td);
                        });
                        
                        table.appendChild(row);
                    });
                    
                    container.appendChild(table);
                })
                .catch(error => {
                    console.error('Error loading latency matrix:', error);
                    showMessage('Error loading latency data', 'error');
                });
        }

        // Load metrics
        function loadMetrics() {
            fetch('/api/metrics')
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    document.getElementById('operations-processed').textContent = data.operations_processed;
                    document.getElementById('avg-latency').textContent = Math.round(data.average_latency);
                    document.getElementById('avg-convergence').textContent = Math.round(data.convergence_time);
                    document.getElementById('network-partitions').textContent = data.network_partitions;
                    
                    // Update uptime
                    const uptime = Math.round(Date.now() / 1000 - data.started_at);
                    document.getElementById('uptime').textContent = `Uptime: ${formatDuration(uptime)}`;
                })
                .catch(error => {
                    console.error('Error loading metrics:', error);
                    showMessage('Error loading metrics data', 'error');
                });
        }
        
        // Load events
        function loadEvents() {
            fetch('/api/events')
                .then(response => {
                    if (!response.ok) {
                        throw new Error(`HTTP error! Status: ${response.status}`);
                    }
                    return response.json();
                })
                .then(data => {
                    const eventsContainer = document.getElementById('events-container');
                    eventsContainer.innerHTML = '';
                    
                    data.events.sort((a, b) => b.timestamp - a.timestamp).forEach(event => {
                        const eventItem = document.createElement('div');
                        eventItem.className = 'event-item';
                        
                        const eventTime = document.createElement('span');
                        eventTime.className = 'event-time';
                        eventTime.textContent = formatTime(event.timestamp);
                        
                        let eventContent;
                        
                        if (event.event_type === 'operation') {
                            eventContent = document.createElement('span');
                            eventContent.className = 'event-operation';
                            
                            if (event.operation_type === 'insert') {
                                eventContent.textContent = `Insert operation (${event.content_length} chars) from ${event.source_node}`;
                            } else {
                                eventContent.textContent = `Delete operation (${event.content_length} chars) from ${event.source_node}`;
                            }
                        } else if (event.event_type === 'sync') {
                            eventContent = document.createElement('span');
                            eventContent.className = 'event-sync';
                            eventContent.textContent = `Synchronization between ${event.source_node} and ${event.target_node}`;
                        } else if (event.event_type === 'partition') {
                            eventContent = document.createElement('span');
                            eventContent.className = 'event-partition';
                            eventContent.textContent = `Network partition between ${event.source_planet} and ${event.target_planet}`;
                        }
                        
                        eventItem.appendChild(eventTime);
                        eventItem.appendChild(document.createTextNode(' '));
                        eventItem.appendChild(eventContent);
                        
                        eventsContainer.appendChild(eventItem);
                    });
                })
                .catch(error => {
                    console.error('Error loading events:', error);
                    showMessage('Error loading event data', 'error');
                });
        }
        
        // Initialize everything
        function loadAll() {
            loadDocument();
            loadNetworkStatus();
            loadLatencyMatrix();
            loadMetrics();
            loadEvents();
        }
        
        // Apply operation
        function applyOperation() {
            const opType = document.getElementById('operation-type').value;
            const position = parseInt(document.getElementById('position').value, 10);
            const content = document.getElementById('content').value;
            
            if (opType === 'insert' && !content) {
                showMessage('Please enter content to insert', 'error');
                return;
            }
            
            fetch('/api/operation', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    op_type: opType,
                    position: position,
                    content: content
                })
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                document.getElementById('document-content').value = data.document;
                document.getElementById('position').value = 0;
                document.getElementById('content').value = '';
                
                showMessage(`Operation applied successfully`);
                
                // Reload relevant data
                loadMetrics();
                loadEvents();
            })
            .catch(error => {
                console.error('Error applying operation:', error);
                showMessage('Error applying operation', 'error');
            });
        }
        
        // Set up event listeners
        document.addEventListener('DOMContentLoaded', () => {
            // Load initial data
            loadAll();
            
            // Set up refresh button
            document.getElementById('refresh-btn').addEventListener('click', loadAll);
            
            // Set up apply operation button
            document.getElementById('apply-btn').addEventListener('click', applyOperation);
            
            // Set up auto-refresh every 10 seconds
            setInterval(loadAll, 10000);
        });
    </script>
</body>
</html>
    """

class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard."""
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path
            
            if path == '/' or path == '/index.html':
                # Serve main dashboard
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(get_html_template().encode())
            
            elif path == '/api/document':
                # Get document content
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "content": document
                }).encode())
            
            elif path == '/api/network':
                # Get network information
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "planets": planets,
                    "nodes": nodes
                }).encode())
            
            elif path == '/api/latency':
                # Get latency information
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "planets": planets,
                    "latencies": latencies
                }).encode())
            
            elif path == '/api/metrics':
                # Get metrics information
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(metrics).encode())
            
            elif path == '/api/events':
                # Get events information
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "events": events[-50:]  # Return last 50 events
                }).encode())
            
            elif path == '/api/status':
                # Get status information
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "running",
                    "node_id": "dashboard-node",
                    "planet_id": "earth",
                    "document_length": len(document),
                    "connected_nodes": sum(1 for node in nodes if node['online']),
                    "uptime": time.time() - metrics['started_at']
                }).encode())
            
            else:
                # Path not found
                self.send_response(404)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Not found')
                
        except Exception as e:
            logger.error(f"Error handling GET request: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Internal server error: {str(e)}".encode())
    
    def do_POST(self):
        """Handle POST requests."""
        try:
            parsed_path = urllib.parse.urlparse(self.path)
            path = parsed_path.path
            
            if path == '/api/operation':
                # Apply operation
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length).decode('utf-8')
                
                try:
                    data = json.loads(post_data)
                    op_type = data.get('op_type')
                    position = int(data.get('position', 0))
                    content = data.get('content', '')
                    
                    result = apply_operation(op_type, position, content)
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                        "document": result
                    }).encode())
                
                except Exception as e:
                    logger.error(f"Error processing operation: {e}")
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "success": False,
                        "error": str(e)
                    }).encode())
            
            else:
                # Path not found
                self.send_response(404)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Not found')
                
        except Exception as e:
            logger.error(f"Error handling POST request: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(f"Internal server error: {str(e)}".encode())

def simulate_network():
    """Simulate network changes for visualization."""
    global nodes, metrics, events
    
    while True:
        time.sleep(10)  # Update every 10 seconds
        
        # Randomly change node status
        for node in nodes:
            if random.random() < 0.1:  # 10% chance to change status
                node['online'] = not node['online']
                node['last_seen'] = time.time()
                
                # Log event for status change
                if not node['online']:
                    events.append({
                        'event_type': 'status',
                        'node_id': node['node_id'],
                        'status': 'offline',
                        'timestamp': time.time()
                    })
                else:
                    events.append({
                        'event_type': 'status',
                        'node_id': node['node_id'],
                        'status': 'online',
                        'timestamp': time.time()
                    })
        
        # Simulate synchronization events
        if random.random() < 0.5:  # 50% chance for sync event
            online_nodes = [node['node_id'] for node in nodes if node['online']]
            if len(online_nodes) >= 2:
                source_node = random.choice(online_nodes)
                target_node = random.choice([n for n in online_nodes if n != source_node])
                
                events.append({
                    'event_type': 'sync',
                    'source_node': source_node,
                    'target_node': target_node,
                    'timestamp': time.time()
                })
                
                metrics['sync_events'] += 1
        
        # Simulate network partition events (less frequent)
        if random.random() < 0.1:  # 10% chance for partition event
            if len(planets) >= 2:
                source_planet = random.choice(planets)
                target_planet = random.choice([p for p in planets if p != source_planet])
                
                events.append({
                    'event_type': 'partition',
                    'source_planet': source_planet,
                    'target_planet': target_planet,
                    'duration': random.randint(10, 60),
                    'timestamp': time.time()
                })
                
                metrics['network_partitions'] += 1
        
        # Update metrics
        metrics['average_latency'] = sum(l['value'] for l in latencies) / len(latencies) if latencies else 0
        
        logger.info("Updated network simulation")

def run_server(port=3000):
    """Run the dashboard server."""
    server_address = ('0.0.0.0', port)
    
    while True:
        try:
            httpd = HTTPServer(server_address, DashboardHandler)
            
            # Start network simulation in a separate thread
            simulation_thread = threading.Thread(target=simulate_network, daemon=True)
            simulation_thread.start()
            
            logger.info(f"Starting dashboard server on port {port}...")
            httpd.serve_forever()
        except OSError as e:
            if e.errno == 98:  # Address already in use
                logger.warning(f"Port {port} is already in use, trying port {port+1}")
                port += 1
                server_address = ('0.0.0.0', port)
            else:
                logger.error(f"Error starting server: {e}")
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            if 'httpd' in locals():
                httpd.server_close()
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if 'httpd' in locals():
                httpd.server_close()
            time.sleep(5)  # Wait a bit before retrying

if __name__ == "__main__":
    run_server()