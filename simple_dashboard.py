"""
Simple Dashboard for Distributed OT Network

This script provides a minimal web dashboard for visualizing the distributed OT network.
"""

import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import logging
import threading
import time
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# In-memory document storage
document = "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transformation.\nYou can edit this text and see how changes are synchronized across the network."
document_history = []

# Network simulation data
planets = ["earth", "mars", "jupiter"]
nodes = [
    {"node_id": "earth-node-1", "planet_id": "earth", "online": True},
    {"node_id": "earth-node-2", "planet_id": "earth", "online": True},
    {"node_id": "mars-node-1", "planet_id": "mars", "online": True},
    {"node_id": "mars-node-2", "planet_id": "mars", "online": True},
    {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True}
]
latencies = [
    {"source": "earth", "target": "mars", "value": 225.0},
    {"source": "mars", "target": "earth", "value": 225.0},
    {"source": "earth", "target": "jupiter", "value": 750.0},
    {"source": "jupiter", "target": "earth", "value": 750.0},
    {"source": "mars", "target": "jupiter", "value": 550.0},
    {"source": "jupiter", "target": "mars", "value": 550.0}
]

def apply_operation(op_type, position, content):
    """Apply an operation to the document."""
    global document
    
    if op_type == 'insert':
        document = document[:position] + content + document[position:]
        document_history.append({
            'type': 'insert',
            'position': position,
            'content': content
        })
    elif op_type == 'delete':
        if position < len(document):
            length = min(len(content) if content else 1, len(document) - position)
            deleted = document[position:position+length]
            document = document[:position] + document[position+length:]
            document_history.append({
                'type': 'delete',
                'position': position,
                'content': deleted
            })
    
    return document

# Read HTML template
def read_template():
    """Read the HTML template."""
    try:
        with open(os.path.join('api', 'templates', 'dashboard.html'), 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading template: {e}")
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Distributed OT Network</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #333; }
                textarea { width: 100%; height: 200px; margin: 10px 0; }
                .controls { margin: 10px 0; }
                button { padding: 5px 10px; background: #4CAF50; color: white; border: none; cursor: pointer; }
                input, select { padding: 5px; margin-right: 10px; }
            </style>
        </head>
        <body>
            <h1>Distributed OT Network Dashboard</h1>
            <h2>Current Document</h2>
            <textarea id="document-content" readonly></textarea>
            <div class="controls">
                <select id="operation-type">
                    <option value="insert">Insert</option>
                    <option value="delete">Delete</option>
                </select>
                <input type="number" id="position" placeholder="Position" min="0" value="0">
                <input type="text" id="content" placeholder="Content to insert">
                <button id="apply-btn">Apply Operation</button>
            </div>
            <h2>Network Status</h2>
            <div id="network-status"></div>
            
            <script>
                // Load document content
                fetch('/api/document')
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('document-content').value = data.content;
                    });
                
                // Apply operation
                document.getElementById('apply-btn').addEventListener('click', function() {
                    const opType = document.getElementById('operation-type').value;
                    const position = parseInt(document.getElementById('position').value);
                    const content = document.getElementById('content').value;
                    
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
                    .then(response => response.json())
                    .then(data => {
                        document.getElementById('document-content').value = data.document;
                        document.getElementById('position').value = 0;
                        document.getElementById('content').value = '';
                    });
                });
                
                // Load network status
                fetch('/api/network')
                    .then(response => response.json())
                    .then(data => {
                        let html = '<ul>';
                        data.nodes.forEach(node => {
                            html += `<li>${node.node_id} (${node.planet_id}): ${node.online ? 'Online' : 'Offline'}</li>`;
                        });
                        html += '</ul>';
                        document.getElementById('network-status').innerHTML = html;
                    });
            </script>
        </body>
        </html>
        """

class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard."""
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path
        
        if path == '/' or path == '/index.html':
            # Serve main dashboard
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(read_template().encode())
        
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
                "connected_nodes": 5
            }).encode())
        
        else:
            # Path not found
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not found')
    
    def do_POST(self):
        """Handle POST requests."""
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

def simulate_network():
    """Simulate network changes for visualization."""
    global nodes
    
    while True:
        time.sleep(10)  # Update every 10 seconds
        
        # Randomly change node status
        for node in nodes:
            if random.random() < 0.2:  # 20% chance to change status
                node['online'] = not node['online']
        
        logger.info("Updated network simulation")

def run_server(port=5000):
    """Run the dashboard server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, DashboardHandler)
    
    # Start network simulation in a separate thread
    simulation_thread = threading.Thread(target=simulate_network, daemon=True)
    simulation_thread.start()
    
    logger.info(f"Starting dashboard server on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        httpd.server_close()

if __name__ == "__main__":
    run_server(port=4000)