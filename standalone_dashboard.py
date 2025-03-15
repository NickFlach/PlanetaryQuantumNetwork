"""
Standalone Dashboard for Distributed OT Network

This script provides a simple, standalone dashboard for the distributed OT network,
with no external dependencies other than standard Python libraries.
"""

import http.server
import socketserver
import json
import threading
import time
import random
import urllib.request
import urllib.error
import logging
import os
from http import HTTPStatus
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
document_content = "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transform technology."
simulation_status = {
    "planets": ["earth", "mars", "jupiter"],
    "nodes": [
        {"node_id": "earth-node-1", "planet_id": "earth", "online": True},
        {"node_id": "earth-node-2", "planet_id": "earth", "online": True},
        {"node_id": "mars-node-1", "planet_id": "mars", "online": True},
        {"node_id": "mars-node-2", "planet_id": "mars", "online": True},
        {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True},
        {"node_id": "jupiter-node-2", "planet_id": "jupiter", "online": True}
    ],
    "latencies": [
        {"source": "earth", "target": "mars", "value": 225.0},
        {"source": "mars", "target": "earth", "value": 225.0},
        {"source": "earth", "target": "jupiter", "value": 750.0},
        {"source": "jupiter", "target": "earth", "value": 750.0},
        {"source": "mars", "target": "jupiter", "value": 550.0},
        {"source": "jupiter", "target": "mars", "value": 550.0}
    ]
}

# HTML template
DASHBOARD_HTML = """
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

        <div class="stats-container">
            <div class="stat-card">
                <h3>Current Planet</h3>
                <div class="stat-value" id="current-planet">Earth</div>
            </div>
            <div class="stat-card">
                <h3>Node ID</h3>
                <div class="stat-value" id="node-id">earth-node-1</div>
            </div>
            <div class="stat-card">
                <h3>Connected Nodes</h3>
                <div class="stat-value" id="connected-nodes">5</div>
            </div>
            <div class="stat-card">
                <h3>Document Length</h3>
                <div class="stat-value" id="document-length">0</div>
            </div>
        </div>

        <h2>Network Topology</h2>
        <div class="planets-grid" id="planets-grid">
            <!-- Planet cards will be added here dynamically -->
        </div>

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
                <button id="apply-operation-btn">Apply Operation</button>
            </div>
            <div id="operation-error" class="error"></div>
        </div>

        <h2>Interplanetary Latency (ms)</h2>
        <table class="latency-matrix" id="latency-matrix">
            <tr>
                <th>From \\ To</th>
                <!-- Planet headers will be added here dynamically -->
            </tr>
            <!-- Latency rows will be added here dynamically -->
        </table>
    </div>

    <script>
        // DOM elements
        const currentPlanetEl = document.getElementById('current-planet');
        const nodeIdEl = document.getElementById('node-id');
        const connectedNodesEl = document.getElementById('connected-nodes');
        const documentLengthEl = document.getElementById('document-length');
        const planetsGridEl = document.getElementById('planets-grid');
        const documentContentEl = document.getElementById('document-content');
        const operationTypeEl = document.getElementById('operation-type');
        const positionEl = document.getElementById('position');
        const contentEl = document.getElementById('content');
        const applyOperationBtn = document.getElementById('apply-operation-btn');
        const refreshBtn = document.getElementById('refresh-btn');
        const operationErrorEl = document.getElementById('operation-error');
        const latencyMatrixEl = document.getElementById('latency-matrix');

        // Show/hide content input based on operation type
        operationTypeEl.addEventListener('change', () => {
            if (operationTypeEl.value === 'delete') {
                contentEl.style.display = 'none';
            } else {
                contentEl.style.display = 'block';
            }
        });

        // Refresh button click handler
        refreshBtn.addEventListener('click', () => {
            loadDashboardData();
        });

        // Apply operation button click handler
        applyOperationBtn.addEventListener('click', async () => {
            try {
                operationErrorEl.textContent = '';
                const operation = {
                    op_type: operationTypeEl.value,
                    position: parseInt(positionEl.value),
                    content: operationTypeEl.value === 'insert' ? contentEl.value : null
                };
                
                const response = await fetch('/api/documents/main/operations', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(operation)
                });
                
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.error || 'Failed to apply operation');
                }
                
                const data = await response.json();
                documentContentEl.value = data.document;
                documentLengthEl.textContent = data.document.length;
                
                // Clear the form
                positionEl.value = 0;
                contentEl.value = '';
            } catch (error) {
                operationErrorEl.textContent = error.message;
            }
        });

        // Load document content
        async function loadDocument() {
            try {
                const response = await fetch('/api/documents/main');
                if (!response.ok) {
                    throw new Error('Failed to load document');
                }
                const data = await response.json();
                documentContentEl.value = data.content;
                documentLengthEl.textContent = data.content.length;
            } catch (error) {
                documentContentEl.value = 'Error loading document: ' + error.message;
            }
        }

        // Load network topology
        async function loadTopology() {
            try {
                const response = await fetch('/api/network/topology');
                if (!response.ok) {
                    throw new Error('Failed to load network topology');
                }
                const data = await response.json();
                
                // Clear existing planet cards
                planetsGridEl.innerHTML = '';
                
                // Create planet cards
                for (const planetId of data.planets) {
                    const planetNodes = data.nodes.filter(node => node.planet_id === planetId);
                    const planetCard = createPlanetCard(planetId, planetNodes);
                    planetsGridEl.appendChild(planetCard);
                }
                
                // Update connected nodes count
                connectedNodesEl.textContent = data.nodes.filter(node => node.online).length - 1; // exclude self
            } catch (error) {
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
            status.textContent = 'Online';
            
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
                const response = await fetch('/api/network/latency');
                if (!response.ok) {
                    throw new Error('Failed to load latency data');
                }
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
            } catch (error) {
                latencyMatrixEl.innerHTML = `<tr><td class="error">Error loading latency data: ${error.message}</td></tr>`;
            }
        }

        // Load status data
        async function loadStatus() {
            try {
                const response = await fetch('/api/status');
                if (!response.ok) {
                    throw new Error('Failed to load status');
                }
                const data = await response.json();
                
                currentPlanetEl.textContent = data.planet_id;
                nodeIdEl.textContent = data.node_id;
            } catch (error) {
                console.error('Error loading status:', error);
            }
        }

        // Load all dashboard data
        async function loadDashboardData() {
            await Promise.all([
                loadStatus(),
                loadDocument(),
                loadTopology(),
                loadLatencyMatrix()
            ]);
        }

        // Initial load
        document.addEventListener('DOMContentLoaded', () => {
            loadDashboardData();
            
            // Refresh data periodically
            setInterval(loadDashboardData, 5000);
        });
    </script>
</body>
</html>
"""

class SimpleDocumentManager:
    """A simple document manager."""
    
    def __init__(self, initial_content: str = ""):
        self.document = initial_content
        
    def apply_operation(self, operation: Dict[str, Any]) -> str:
        """Apply an operation to the document."""
        op_type = operation.get('op_type')
        position = operation.get('position', 0)
        content = operation.get('content', '')
        
        if op_type == 'insert':
            if position < 0 or position > len(self.document):
                raise ValueError(f"Invalid position {position} for document of length {len(self.document)}")
            self.document = self.document[:position] + content + self.document[position:]
        elif op_type == 'delete':
            if position < 0 or position >= len(self.document):
                raise ValueError(f"Invalid position {position} for document of length {len(self.document)}")
            # Delete one character or a specified range
            if content and len(content) > 0:
                if position + len(content) <= len(self.document):
                    self.document = self.document[:position] + self.document[position+len(content):]
            else:
                # Default to deleting one character
                self.document = self.document[:position] + self.document[position+1:]
                
        return self.document

class StandaloneDashboardHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler for the standalone dashboard."""
    
    def __init__(self, *args, document_manager=None, **kwargs):
        self.document_manager = document_manager
        super().__init__(*args, **kwargs)
    
    def log_message(self, format, *args):
        logger.info(format % args)
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/':
            # Serve dashboard HTML
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode())
        elif self.path == '/api/status':
            self._handle_status()
        elif self.path == '/api/documents/main':
            self._handle_get_document()
        elif self.path == '/api/network/topology':
            self._handle_network_topology()
        elif self.path == '/api/network/latency':
            self._handle_network_latency()
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
    
    def do_POST(self):
        """Handle POST requests."""
        if self.path == '/api/documents/main/operations':
            self._handle_apply_operation()
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
    
    def _send_json_response(self, data: Dict[str, Any], status: int = HTTPStatus.OK):
        """Send a JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def _handle_status(self):
        """Handle status request."""
        self._send_json_response({
            "success": True,
            "planet_id": "earth",
            "node_id": "earth-node-1",
            "connected_nodes": 5,
            "document_length": len(self.document_manager.document),
            "version": "1.0.0"
        })
    
    def _handle_get_document(self):
        """Handle document retrieval."""
        self._send_json_response({
            "success": True,
            "content": self.document_manager.document
        })
    
    def _handle_apply_operation(self):
        """Handle operation application."""
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            operation = json.loads(post_data.decode())
            
            document = self.document_manager.apply_operation(operation)
            
            self._send_json_response({
                "success": True,
                "document": document
            })
        except Exception as e:
            logger.error(f"Error applying operation: {e}")
            self._send_json_response({
                "error": str(e)
            }, HTTPStatus.INTERNAL_SERVER_ERROR)
    
    def _handle_network_topology(self):
        """Handle network topology request."""
        self._send_json_response({
            "planets": simulation_status["planets"],
            "nodes": simulation_status["nodes"]
        })
    
    def _handle_network_latency(self):
        """Handle network latency request."""
        self._send_json_response({
            "planets": simulation_status["planets"],
            "latencies": simulation_status["latencies"]
        })

def start_server(port=5000):
    """Start the standalone dashboard server."""
    # Create document manager with initial content
    doc_manager = SimpleDocumentManager(document_content)
    
    # Custom handler factory that includes the document manager
    def handler_factory(*args, **kwargs):
        return StandaloneDashboardHandler(*args, document_manager=doc_manager, **kwargs)
    
    # Create and start server
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", port), handler_factory) as httpd:
        logger.info(f"Standalone dashboard server running at port {port}")
        
        # Start a background thread to simulate network changes
        threading.Thread(target=simulate_network_changes, daemon=True).start()
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Server stopped")

def simulate_network_changes():
    """Periodically simulate changes in the network."""
    while True:
        try:
            # Randomly change some node statuses
            for node in simulation_status["nodes"]:
                if random.random() < 0.1:  # 10% chance to change status
                    node["online"] = not node["online"]
                    
            # Randomly adjust latencies (±10%)
            for latency in simulation_status["latencies"]:
                adjustment = random.uniform(0.9, 1.1)
                latency["value"] = round(latency["value"] * adjustment, 1)
                
            # Sleep for a few seconds
            time.sleep(random.uniform(3, 8))
        except Exception as e:
            logger.error(f"Error in network simulation: {e}")
            time.sleep(5)  # Sleep on error

def check_simulation_servers():
    """Try to fetch data from simulation servers."""
    global simulation_status
    
    servers = [
        "http://localhost:5000",
        "http://localhost:5001",
        "http://localhost:5002"
    ]
    
    for server_url in servers:
        try:
            # Try to fetch network topology
            with urllib.request.urlopen(f"{server_url}/network/topology", timeout=2) as response:
                data = json.loads(response.read().decode())
                if 'planets' in data and 'nodes' in data:
                    simulation_status["planets"] = data["planets"]
                    simulation_status["nodes"] = data["nodes"]
                    logger.info(f"Connected to simulation server at {server_url}")
                    break
        except (urllib.error.URLError, json.JSONDecodeError, ConnectionRefusedError) as e:
            logger.debug(f"Could not connect to simulation server at {server_url}: {e}")
    
    # If we couldn't get any real data, use our simulated data
    if not simulation_status.get("planets"):
        logger.warning("Could not connect to any simulation servers, using simulated data")

if __name__ == "__main__":
    # Check if any simulation servers are running
    check_simulation_servers()
    
    # Start the dashboard server
    port = int(os.environ.get('PORT', 3000))
    start_server(port)