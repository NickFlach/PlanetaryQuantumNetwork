"""
Ultra Reliable Dashboard for Distributed OT Network

This script provides an extremely reliable web dashboard for visualizing the distributed OT network
using only standard Python libraries with no external dependencies.
"""

import os
import json
import logging
import time
import random
import http.server
import socketserver
import threading
import urllib.request
import urllib.error
from http import HTTPStatus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# HTML Template
HTML_TEMPLATE = """<!DOCTYPE html>
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
        }
        button {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-weight: bold;
        }
        button:hover {
            background-color: #2980b9;
        }
        input, select {
            padding: 10px;
            border-radius: 4px;
            border: 1px solid #ddd;
        }
        .latency-matrix {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        .latency-matrix th, .latency-matrix td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        .latency-matrix th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        .refresh-btn {
            background-color: #2ecc71;
        }
        .refresh-btn:hover {
            background-color: #27ae60;
        }
        .error {
            color: #e74c3c;
            font-weight: bold;
            margin-top: 10px;
        }
        .server-selector {
            margin-bottom: 20px;
        }
        .status-container {
            margin-bottom: 20px;
            padding: 10px;
            border-radius: 4px;
            background-color: #f8f9fa;
        }
        .status-online {
            color: #27ae60;
        }
        .status-offline {
            color: #e74c3c;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Distributed OT Network Dashboard</h1>
            <div>
                <button class="refresh-btn" id="refresh-btn">Refresh Data</button>
            </div>
        </div>

        <div class="server-selector">
            <label for="server-url">Server URL:</label>
            <select id="server-url">
                <option value="http://localhost:5000">Simulation 1 (Port 5000)</option>
                <option value="http://localhost:5001">Simulation 2 (Port 5001)</option>
                <option value="http://localhost:5002">Large Simulation (Port 5002)</option>
            </select>
            <button id="connect-btn">Connect</button>
        </div>

        <div class="status-container">
            <h3>Server Status: <span id="server-status" class="status-offline">Disconnected</span></h3>
            <div id="status-message"></div>
        </div>

        <div class="stats-container">
            <div class="stat-card">
                <h3>Current Planet</h3>
                <div class="stat-value" id="current-planet">-</div>
            </div>
            <div class="stat-card">
                <h3>Node ID</h3>
                <div class="stat-value" id="node-id">-</div>
            </div>
            <div class="stat-card">
                <h3>Connected Nodes</h3>
                <div class="stat-value" id="connected-nodes">-</div>
            </div>
            <div class="stat-card">
                <h3>Document Length</h3>
                <div class="stat-value" id="document-length">-</div>
            </div>
        </div>

        <h2>Network Topology</h2>
        <div class="planets-grid" id="planets-grid">
            <!-- Planet cards will be added here dynamically -->
        </div>

        <h2>Interplanetary Latency (ms)</h2>
        <table class="latency-matrix" id="latency-matrix">
            <tr>
                <th>From \\ To</th>
                <!-- Planet headers will be added here dynamically -->
            </tr>
            <!-- Latency rows will be added here dynamically -->
        </table>

        <h2>Document Editor</h2>
        <div class="document-section">
            <textarea id="document-content" readonly>Loading document content...</textarea>
            <div class="operation-form">
                <select id="operation-type">
                    <option value="insert">Insert</option>
                    <option value="delete">Delete</option>
                </select>
                <input type="number" id="position" placeholder="Position" min="0" value="0">
                <input type="text" id="content" placeholder="Content to insert">
                <button id="apply-operation-btn" disabled>Apply Operation</button>
            </div>
            <div id="operation-error" class="error"></div>
        </div>
    </div>

    <script>
        // DOM elements
        const serverUrlEl = document.getElementById('server-url');
        const connectBtnEl = document.getElementById('connect-btn');
        const serverStatusEl = document.getElementById('server-status');
        const statusMessageEl = document.getElementById('status-message');
        const currentPlanetEl = document.getElementById('current-planet');
        const nodeIdEl = document.getElementById('node-id');
        const connectedNodesEl = document.getElementById('connected-nodes');
        const documentLengthEl = document.getElementById('document-length');
        const planetsGridEl = document.getElementById('planets-grid');
        const documentContentEl = document.getElementById('document-content');
        const operationTypeEl = document.getElementById('operation-type');
        const positionEl = document.getElementById('position');
        const contentEl = document.getElementById('content');
        const applyOperationBtnEl = document.getElementById('apply-operation-btn');
        const refreshBtnEl = document.getElementById('refresh-btn');
        const operationErrorEl = document.getElementById('operation-error');
        const latencyMatrixEl = document.getElementById('latency-matrix');

        // Current server URL
        let currentServerUrl = null;

        // Show/hide content input based on operation type
        operationTypeEl.addEventListener('change', () => {
            if (operationTypeEl.value === 'delete') {
                contentEl.style.display = 'none';
            } else {
                contentEl.style.display = 'block';
            }
        });

        // Connect button click handler
        connectBtnEl.addEventListener('click', () => {
            currentServerUrl = serverUrlEl.value;
            serverStatusEl.textContent = "Connecting...";
            serverStatusEl.className = "";
            statusMessageEl.textContent = `Attempting to connect to ${currentServerUrl}...`;
            
            // Try to load data from the selected server
            loadDashboardData();
        });

        // Refresh button click handler
        refreshBtnEl.addEventListener('click', () => {
            if (currentServerUrl) {
                serverStatusEl.textContent = "Refreshing...";
                loadDashboardData();
            } else {
                alert("Please connect to a server first");
            }
        });

        // Apply operation button click handler
        applyOperationBtnEl.addEventListener('click', () => {
            const operationType = operationTypeEl.value;
            const position = parseInt(positionEl.value);
            const content = contentEl.value;
            
            if (isNaN(position) || position < 0) {
                operationErrorEl.textContent = "Position must be a non-negative number";
                return;
            }
            
            if (operationType === 'insert' && !content) {
                operationErrorEl.textContent = "Content is required for insert operations";
                return;
            }
            
            operationErrorEl.textContent = "";
            
            applyOperation(operationType, position, content);
        });

        // Apply an operation to the document
        async function applyOperation(operationType, position, content) {
            if (!currentServerUrl) {
                operationErrorEl.textContent = "Not connected to any server";
                return;
            }
            
            try {
                const response = await fetch(`${currentServerUrl}/documents/main/operations`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        op_type: operationType,
                        position: position,
                        content: content
                    })
                });
                
                if (response.ok) {
                    const data = await response.json();
                    documentContentEl.value = data.document;
                    // Clear inputs
                    positionEl.value = "0";
                    contentEl.value = "";
                } else {
                    const error = await response.text();
                    operationErrorEl.textContent = `Error: ${error}`;
                }
            } catch (error) {
                operationErrorEl.textContent = `Network error: ${error.message}`;
            }
        }

        // Load status information
        async function loadStatus() {
            try {
                const response = await fetch(`${currentServerUrl}/status`);
                if (response.ok) {
                    const data = await response.json();
                    
                    // Update status indicators
                    serverStatusEl.textContent = "Connected";
                    serverStatusEl.className = "status-online";
                    statusMessageEl.textContent = `Connected to ${currentServerUrl}`;
                    
                    // Update stats
                    currentPlanetEl.textContent = data.planet_id || "-";
                    nodeIdEl.textContent = data.node_id || "-";
                    connectedNodesEl.textContent = data.connected_nodes || "-";
                    documentLengthEl.textContent = data.document_length || "-";
                    
                    // Enable operation button
                    applyOperationBtnEl.disabled = false;
                    
                    return true;
                } else {
                    throw new Error(`Server returned ${response.status}`);
                }
            } catch (error) {
                console.error("Error fetching status:", error);
                serverStatusEl.textContent = "Disconnected";
                serverStatusEl.className = "status-offline";
                statusMessageEl.textContent = `Failed to connect to ${currentServerUrl}: ${error.message}`;
                applyOperationBtnEl.disabled = true;
                return false;
            }
        }

        // Load document content
        async function loadDocument() {
            try {
                const response = await fetch(`${currentServerUrl}/documents/main`);
                if (response.ok) {
                    const data = await response.json();
                    documentContentEl.value = data.content || "No content available";
                } else {
                    throw new Error(`Server returned ${response.status}`);
                }
            } catch (error) {
                console.error("Error fetching document:", error);
                documentContentEl.value = "Could not load document content. Please check server connection.";
            }
        }

        // Load network topology
        async function loadTopology() {
            try {
                const response = await fetch(`${currentServerUrl}/network/topology`);
                if (response.ok) {
                    const data = await response.json();
                    
                    // Clear existing planet cards
                    planetsGridEl.innerHTML = '';
                    
                    // Group nodes by planet
                    const planetNodes = {};
                    for (const planet of data.planets) {
                        planetNodes[planet] = [];
                    }
                    
                    for (const node of data.nodes) {
                        if (planetNodes[node.planet_id]) {
                            planetNodes[node.planet_id].push(node);
                        }
                    }
                    
                    // Create planet cards
                    for (const planetId of data.planets) {
                        const planetCard = createPlanetCard(planetId, planetNodes[planetId] || []);
                        planetsGridEl.appendChild(planetCard);
                    }
                } else {
                    throw new Error(`Server returned ${response.status}`);
                }
            } catch (error) {
                console.error("Error fetching topology:", error);
                planetsGridEl.innerHTML = `<div class="error">Error loading topology: ${error.message}</div>`;
            }
        }

        // Create a planet card element
        function createPlanetCard(planetId, nodes) {
            const card = document.createElement('div');
            card.className = 'planet-card';
            
            const header = document.createElement('div');
            header.className = 'planet-header';
            
            const name = document.createElement('div');
            name.className = 'planet-name';
            name.textContent = `Planet: ${planetId}`;
            
            const status = document.createElement('div');
            status.className = 'planet-status';
            
            // Check if any node on this planet is online
            const anyNodeOnline = nodes.some(node => node.online);
            status.textContent = anyNodeOnline ? 'Online' : 'Offline';
            status.style.backgroundColor = anyNodeOnline ? '#27ae60' : '#e74c3c';
            
            header.appendChild(name);
            header.appendChild(status);
            
            const nodesList = document.createElement('div');
            nodesList.className = 'nodes-list';
            
            nodes.forEach(node => {
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
            
            card.appendChild(header);
            card.appendChild(nodesList);
            
            return card;
        }

        // Load latency matrix
        async function loadLatencyMatrix() {
            try {
                const response = await fetch(`${currentServerUrl}/network/latency`);
                if (response.ok) {
                    const data = await response.json();
                    
                    // Clear existing table
                    latencyMatrixEl.innerHTML = '';
                    
                    // Add header row
                    const headerRow = document.createElement('tr');
                    const cornerCell = document.createElement('th');
                    cornerCell.textContent = 'From \\ To';
                    headerRow.appendChild(cornerCell);
                    
                    for (const planet of data.planets) {
                        const th = document.createElement('th');
                        th.textContent = planet;
                        headerRow.appendChild(th);
                    }
                    
                    latencyMatrixEl.appendChild(headerRow);
                    
                    // Add data rows
                    for (const sourcePlanet of data.planets) {
                        const row = document.createElement('tr');
                        
                        const labelCell = document.createElement('th');
                        labelCell.textContent = sourcePlanet;
                        row.appendChild(labelCell);
                        
                        for (const targetPlanet of data.planets) {
                            const td = document.createElement('td');
                            if (sourcePlanet === targetPlanet) {
                                td.textContent = '0';
                                td.style.backgroundColor = '#f2f2f2';
                            } else {
                                const latency = data.latencies.find(l => 
                                    l.source === sourcePlanet && l.target === targetPlanet);
                                td.textContent = latency ? latency.value.toFixed(1) : 'N/A';
                            }
                            row.appendChild(td);
                        }
                        
                        latencyMatrixEl.appendChild(row);
                    }
                } else {
                    throw new Error(`Server returned ${response.status}`);
                }
            } catch (error) {
                console.error("Error fetching latency data:", error);
                latencyMatrixEl.innerHTML = `<tr><td class="error">Error loading latency data: ${error.message}</td></tr>`;
            }
        }

        // Load all dashboard data
        async function loadDashboardData() {
            // First check if we can connect
            const connected = await loadStatus();
            
            if (connected) {
                // Load all other data
                await Promise.all([
                    loadDocument(),
                    loadTopology(),
                    loadLatencyMatrix()
                ]);
            }
        }

        // Initial setup - trigger content visibility based on operation type
        operationTypeEl.dispatchEvent(new Event('change'));
    </script>
</body>
</html>
"""

class SimpleDocumentManager:
    """A simple document manager for the dashboard."""
    
    def __init__(self):
        self.document = "Welcome to the Distributed OT Network Dashboard!\nThis collaborative editor is powered by Operational Transform technology."
        self.history = []
    
    def apply_operation(self, operation):
        """Apply an operation to the document."""
        op_type = operation.get('op_type')
        position = operation.get('position', 0)
        content = operation.get('content', '')
        
        if op_type == 'insert':
            # Insert content at the specified position
            self.document = self.document[:position] + content + self.document[position:]
            
        elif op_type == 'delete':
            # Delete content starting at the specified position
            delete_length = len(content) if content else 1
            if position < len(self.document):
                if position + delete_length <= len(self.document):
                    deleted = self.document[position:position+delete_length]
                    self.document = self.document[:position] + self.document[position+delete_length:]
        
        # Add to history
        self.history.append({
            'type': op_type,
            'position': position,
            'content': content,
            'timestamp': time.time()
        })
        
        return self.document

class UltraReliableDashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Handler for the ultra reliable dashboard."""
    
    # Class-level shared data for all handler instances
    shared_data = {
        'planets': ["earth", "mars"],
        'nodes': [
            {"node_id": "earth-node-0", "planet_id": "earth", "online": True},
            {"node_id": "earth-node-1", "planet_id": "earth", "online": True},
            {"node_id": "mars-node-0", "planet_id": "mars", "online": True},
            {"node_id": "mars-node-1", "planet_id": "mars", "online": True}
        ],
        'latencies': [
            {"source": "earth", "target": "mars", "value": 225.0},
            {"source": "mars", "target": "earth", "value": 225.0}
        ]
    }
    
    def __init__(self, *args, document_manager=None, **kwargs):
        self.document_manager = document_manager or SimpleDocumentManager()
        # Call parent constructor first, avoiding the original issue
        super().__init__(*args, **kwargs)
    
    def log_message(self, format, *args):
        # Use the configured logger instead of printing to stderr
        logger.info(format % args)
    
    def do_GET(self):
        """Handle GET requests."""
        # Check API endpoints
        if self.path == '/':
            self._send_dashboard()
        elif self.path == '/status':
            self._handle_status()
        elif self.path == '/documents/main':
            self._handle_get_document()
        elif self.path == '/network/topology':
            self._handle_network_topology()
        elif self.path == '/network/latency':
            self._handle_network_latency()
        else:
            # Return 404 for all other paths
            self.send_error(HTTPStatus.NOT_FOUND, "Resource not found")
    
    def do_POST(self):
        """Handle POST requests."""
        if self.path == '/documents/main/operations':
            self._handle_apply_operation()
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Resource not found")
    
    def _send_dashboard(self):
        """Send the dashboard HTML."""
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(HTML_TEMPLATE.encode('utf-8'))
    
    def _send_json_response(self, data, status=HTTPStatus.OK):
        """Send a JSON response."""
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))
    
    def _handle_status(self):
        """Handle status request."""
        nodes = self.shared_data['nodes']
        status_data = {
            "success": True,
            "planet_id": "earth",
            "node_id": nodes[0]["node_id"] if nodes else "unknown",
            "connected_nodes": len(nodes) - 1 if nodes else 0,
            "document_length": len(self.document_manager.document),
            "version": "1.0.0"
        }
        self._send_json_response(status_data)
    
    def _handle_get_document(self):
        """Handle document retrieval."""
        self._send_json_response({
            "success": True,
            "content": self.document_manager.document
        })
    
    def _handle_apply_operation(self):
        """Handle operation application."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
            try:
                operation = json.loads(body.decode('utf-8'))
                document = self.document_manager.apply_operation(operation)
                self._send_json_response({
                    "success": True,
                    "document": document
                })
            except json.JSONDecodeError:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid JSON")
            except Exception as e:
                logger.error(f"Error applying operation: {e}")
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
        else:
            self.send_error(HTTPStatus.BAD_REQUEST, "No operation data provided")
    
    def _handle_network_topology(self):
        """Handle network topology request."""
        # Try to retrieve topology from a real simulation server
        real_topology = self._fetch_real_topology()
        
        # If we got real data, update our cached values
        if real_topology:
            if 'planets' in real_topology:
                self.shared_data['planets'] = real_topology['planets']
            if 'nodes' in real_topology:
                self.shared_data['nodes'] = real_topology['nodes']
        
        self._send_json_response({
            "planets": self.shared_data['planets'],
            "nodes": self.shared_data['nodes']
        })
    
    def _handle_network_latency(self):
        """Handle network latency request."""
        # Try to retrieve latency from a real simulation server
        real_latency = self._fetch_real_latency()
        
        # If we got real data, update our cached values
        if real_latency:
            if 'planets' in real_latency:
                self.shared_data['planets'] = real_latency['planets']
            if 'latencies' in real_latency:
                self.shared_data['latencies'] = real_latency['latencies']
        
        self._send_json_response({
            "planets": self.shared_data['planets'],
            "latencies": self.shared_data['latencies']
        })
    
    def _fetch_real_topology(self):
        """Fetch real topology data from simulation servers."""
        # Try each of our known simulation ports
        for port in [5000, 5001, 5002]:
            try:
                with urllib.request.urlopen(f"http://localhost:{port}/network/topology", timeout=1) as response:
                    if response.status == 200:
                        data = json.loads(response.read().decode('utf-8'))
                        logger.info(f"Successfully fetched topology from simulation on port {port}")
                        return data
            except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
                logger.debug(f"Could not fetch topology from port {port}: {e}")
        
        return None
    
    def _fetch_real_latency(self):
        """Fetch real latency data from simulation servers."""
        # Try each of our known simulation ports
        for port in [5000, 5001, 5002]:
            try:
                with urllib.request.urlopen(f"http://localhost:{port}/network/latency", timeout=1) as response:
                    if response.status == 200:
                        data = json.loads(response.read().decode('utf-8'))
                        logger.info(f"Successfully fetched latency from simulation on port {port}")
                        return data
            except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
                logger.debug(f"Could not fetch latency from port {port}: {e}")
        
        return None

def run_server(port=4000):
    """Run the dashboard server."""
    document_manager = SimpleDocumentManager()
    
    # Custom handler factory to pass document manager
    def handler_factory(*args, **kwargs):
        return UltraReliableDashboardHandler(*args, document_manager=document_manager, **kwargs)
    
    # Start server
    try:
        server = socketserver.ThreadingTCPServer(('0.0.0.0', port), handler_factory)
        logger.info(f"Ultra reliable dashboard running at http://0.0.0.0:{port}")
        
        # Start a thread to periodically check for real simulation data
        threading.Thread(target=update_cached_data, args=(handler_factory,), daemon=True).start()
        
        server.serve_forever()
    except OSError as e:
        if e.errno == 98:  # Address already in use
            logger.error(f"Port {port} is already in use. Try another port.")
            return False
        else:
            raise
    except KeyboardInterrupt:
        server.shutdown()
        logger.info("Server stopped")
    
    return True

def update_cached_data(document_manager):
    """Periodically update cached data from real simulation servers."""
    # Create a handler class that doesn't need initialization
    class DataFetcher:
        def _fetch_real_topology(self):
            """Fetch real topology data from simulation servers."""
            # Try each of our known simulation ports
            for port in [5000, 5001, 5002]:
                try:
                    with urllib.request.urlopen(f"http://localhost:{port}/network/topology", timeout=1) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode('utf-8'))
                            logger.info(f"Successfully fetched topology from simulation on port {port}")
                            return data
                except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
                    logger.debug(f"Could not fetch topology from port {port}: {e}")
            return None
        
        def _fetch_real_latency(self):
            """Fetch real latency data from simulation servers."""
            # Try each of our known simulation ports
            for port in [5000, 5001, 5002]:
                try:
                    with urllib.request.urlopen(f"http://localhost:{port}/network/latency", timeout=1) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode('utf-8'))
                            logger.info(f"Successfully fetched latency from simulation on port {port}")
                            return data
                except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
                    logger.debug(f"Could not fetch topology from port {port}: {e}")
            return None
    
    # Create an instance of our fetcher
    fetcher = DataFetcher()
    
    # Placeholder data that will be shared with all handler instances
    shared_data = {
        'planets': ["earth", "mars"],
        'nodes': [
            {"node_id": "earth-node-0", "planet_id": "earth", "online": True},
            {"node_id": "earth-node-1", "planet_id": "earth", "online": True},
            {"node_id": "mars-node-0", "planet_id": "mars", "online": True},
            {"node_id": "mars-node-1", "planet_id": "mars", "online": True}
        ],
        'latencies': [
            {"source": "earth", "target": "mars", "value": 225.0},
            {"source": "mars", "target": "earth", "value": 225.0}
        ]
    }
    
    while True:
        try:
            # Update topology
            topology = fetcher._fetch_real_topology()
            if topology:
                if 'planets' in topology:
                    shared_data['planets'] = topology['planets']
                if 'nodes' in topology:
                    shared_data['nodes'] = topology['nodes']
            
            # Update latency
            latency = fetcher._fetch_real_latency()
            if latency:
                if 'planets' in latency:
                    shared_data['planets'] = latency['planets']
                if 'latencies' in latency:
                    shared_data['latencies'] = latency['latencies']
            
            # Make this data available to all handlers
            UltraReliableDashboardHandler.shared_data = shared_data
            
            # Wait before checking again (10-15 seconds)
            time.sleep(random.uniform(10, 15))
        except Exception as e:
            logger.error(f"Error updating cached data: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # Try different ports if one is in use
    ports_to_try = [4000, 4001, 4002, 4003]
    
    for port in ports_to_try:
        if run_server(port):
            break