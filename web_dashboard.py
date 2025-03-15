"""
Web Dashboard for Distributed OT Network

This script creates a simple web dashboard for visualization of the 
distributed operational transform network.
"""

import asyncio
import logging
import json
import os
from aiohttp import web
import aiohttp_jinja2
import jinja2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentManager:
    """Simple document manager for the dashboard demo."""
    
    def __init__(self):
        self.document = "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transformation.\nYou can edit this text and see how changes are synchronized across the network."
        self.document_id = "main"
        self.history = []
    
    def apply_operation(self, operation):
        """Apply an operation to the document."""
        op_type = operation.get('op_type')
        position = operation.get('position', 0)
        content = operation.get('content', '')
        
        if op_type == 'insert':
            self.document = self.document[:position] + content + self.document[position:]
            self.history.append({
                'type': 'insert',
                'position': position,
                'content': content
            })
        elif op_type == 'delete':
            if position < len(self.document):
                length = min(len(content) if content else 1, len(self.document) - position)
                deleted = self.document[position:position+length]
                self.document = self.document[:position] + self.document[position+length:]
                self.history.append({
                    'type': 'delete',
                    'position': position,
                    'content': deleted
                })
        
        return self.document

class Dashboard:
    """Web dashboard for the OT network visualization."""
    
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.document_manager = DocumentManager()
        self.setup_routes()
        self.setup_jinja()
    
    def setup_jinja(self):
        """Set up Jinja2 templates."""
        template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api', 'templates')
        aiohttp_jinja2.setup(self.app, loader=jinja2.FileSystemLoader(template_dir))
    
    def setup_routes(self):
        """Set up routes for the dashboard."""
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/status', self.handle_status)
        self.app.router.add_get('/documents/{document_id}', self.handle_get_document)
        self.app.router.add_post('/documents/{document_id}/operations', self.handle_apply_operation)
        self.app.router.add_get('/network/topology', self.handle_network_topology)
        self.app.router.add_get('/network/latency', self.handle_network_latency)
    
    async def handle_index(self, request):
        """Handle index route."""
        if request.headers.get('Accept') == 'application/json':
            return web.json_response({
                "name": "Distributed OT Network Dashboard",
                "version": "1.0.0",
                "status": "running",
                "planet": "earth"
            })
        
        # For browsers, serve the dashboard HTML
        try:
            with open(os.path.join('api', 'templates', 'dashboard.html'), 'r') as f:
                html_content = f.read()
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"Error serving dashboard: {e}")
            return web.Response(text=f"""
            <html>
                <head><title>Distributed OT Network</title></head>
                <body>
                    <h1>Distributed OT Network</h1>
                    <p>Planet: earth</p>
                    <p>Node: dashboard-node</p>
                    <p>Status: Running</p>
                    <p style="color: red;">Error loading dashboard: {str(e)}</p>
                </body>
            </html>
            """, content_type='text/html')
    
    async def handle_status(self, request):
        """Handle status route."""
        return web.json_response({
            "status": "running",
            "node_id": "dashboard-node",
            "planet_id": "earth",
            "document_length": len(self.document_manager.document),
            "connected_nodes": 5  # Simulated connected nodes
        })
    
    async def handle_get_document(self, request):
        """Handle document retrieval."""
        document_id = request.match_info.get('document_id', 'main')
        
        return web.json_response({
            "success": True,
            "content": self.document_manager.document
        })
    
    async def handle_apply_operation(self, request):
        """Handle operation application."""
        document_id = request.match_info.get('document_id', 'main')
        
        try:
            # Extract operation details
            data = await request.json()
            
            # Apply operation to our document manager
            new_content = self.document_manager.apply_operation(data)
            
            # Return success response with the updated document
            return web.json_response({
                "success": True,
                "document": new_content
            })
        except Exception as e:
            logger.error(f"Error applying operation: {e}")
            return web.json_response({
                "error": f"Failed to apply operation: {str(e)}"
            }, status=500)
    
    async def handle_network_topology(self, request):
        """Handle network topology request."""
        return web.json_response({
            "planets": ["earth", "mars", "jupiter"],
            "nodes": [
                {"node_id": "earth-node-1", "planet_id": "earth", "online": True},
                {"node_id": "earth-node-2", "planet_id": "earth", "online": True},
                {"node_id": "mars-node-1", "planet_id": "mars", "online": True},
                {"node_id": "mars-node-2", "planet_id": "mars", "online": True},
                {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True}
            ]
        })
    
    async def handle_network_latency(self, request):
        """Handle network latency request."""
        return web.json_response({
            "planets": ["earth", "mars", "jupiter"],
            "latencies": [
                {"source": "earth", "target": "mars", "value": 225.0},
                {"source": "mars", "target": "earth", "value": 225.0},
                {"source": "earth", "target": "jupiter", "value": 750.0},
                {"source": "jupiter", "target": "earth", "value": 750.0},
                {"source": "mars", "target": "jupiter", "value": 550.0},
                {"source": "jupiter", "target": "mars", "value": 550.0}
            ]
        })
    
    async def start(self):
        """Start the dashboard server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info(f"Dashboard server running at http://{self.host}:{self.port}")
        return runner
    
    async def run(self):
        """Run the dashboard server."""
        runner = await self.start()
        
        # Keep running until interrupted
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            await runner.cleanup()
            logger.info("Dashboard server shutdown complete")

async def main():
    """Main entry point."""
    dashboard = Dashboard(port=3000)
    await dashboard.run()

if __name__ == "__main__":
    asyncio.run(main())