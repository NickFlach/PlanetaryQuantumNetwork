"""
Reliable Dashboard for Distributed OT Network

This script provides a reliable web dashboard for visualizing the distributed OT network
without any external SIN dependencies.
"""

import asyncio
import logging
import json
import os
import aiohttp
from aiohttp import web
import jinja2
import aiohttp_jinja2
import uuid
from typing import Dict, Any, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleDocumentManager:
    """A simplified document manager for the dashboard."""
    
    def __init__(self):
        self.document = "Welcome to the Distributed OT Network Dashboard!\n\nThis is a collaborative text editor powered by Operational Transform technology."
        self.history = []
    
    def apply_operation(self, operation: Dict[str, Any]) -> str:
        """Apply an operation to the document."""
        op_type = operation.get('op_type')
        position = operation.get('position', 0)
        content = operation.get('content', '')
        
        if op_type == 'insert':
            # Insert content at the specified position
            self.document = self.document[:position] + content + self.document[position:]
            self.history.append({
                'type': 'insert',
                'position': position,
                'content': content
            })
        elif op_type == 'delete':
            # Delete content starting at the specified position
            delete_length = len(content) if content else 1
            if position < len(self.document):
                if position + delete_length <= len(self.document):
                    deleted = self.document[position:position+delete_length]
                    self.document = self.document[:position] + self.document[position+delete_length:]
                    self.history.append({
                        'type': 'delete',
                        'position': position,
                        'content': deleted
                    })
        
        return self.document

class ReliableDashboard:
    """Web dashboard for the OT network visualization."""
    
    def __init__(self, host='0.0.0.0', port=5000):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.doc_manager = SimpleDocumentManager()
        self.running = False
        
        # Mock network data
        self.planets = ["earth", "mars", "jupiter"]
        self.nodes = [
            {"node_id": f"earth-node-{str(uuid.uuid4())[:8]}", "planet_id": "earth", "online": True},
            {"node_id": "mars-node-1", "planet_id": "mars", "online": True},
            {"node_id": "mars-node-2", "planet_id": "mars", "online": True},
            {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True}
        ]
        self.latencies = [
            {"source": "earth", "target": "mars", "value": 225.0},
            {"source": "mars", "target": "earth", "value": 225.0},
            {"source": "earth", "target": "jupiter", "value": 750.0},
            {"source": "jupiter", "target": "earth", "value": 750.0},
            {"source": "mars", "target": "jupiter", "value": 550.0},
            {"source": "jupiter", "target": "mars", "value": 550.0}
        ]
        
        # Setup routes and templates
        self.setup_jinja()
        self.setup_routes()

    def setup_jinja(self):
        """Set up Jinja2 templates."""
        template_dir = os.path.join(os.path.dirname(__file__), 'api', 'templates')
        aiohttp_jinja2.setup(
            self.app,
            loader=jinja2.FileSystemLoader(template_dir)
        )
    
    def setup_routes(self):
        """Set up routes for the dashboard."""
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/status', self.handle_status)
        self.app.router.add_get('/documents/main', self.handle_get_document)
        self.app.router.add_post('/documents/main/operations', self.handle_apply_operation)
        self.app.router.add_get('/network/topology', self.handle_network_topology)
        self.app.router.add_get('/network/latency', self.handle_network_latency)
        
    async def handle_index(self, request):
        """Handle index route."""
        return aiohttp_jinja2.render_template('dashboard.html', request, {})
        
    async def handle_status(self, request):
        """Handle status route."""
        return web.json_response({
            "success": True,
            "planet_id": "earth",
            "node_id": self.nodes[0]["node_id"],
            "connected_nodes": len(self.nodes) - 1,
            "document_length": len(self.doc_manager.document),
            "version": "1.0.0"
        })
        
    async def handle_get_document(self, request):
        """Handle document retrieval."""
        return web.json_response({
            "success": True,
            "content": self.doc_manager.document
        })
        
    async def handle_apply_operation(self, request):
        """Handle operation application."""
        try:
            data = await request.json()
            document = self.doc_manager.apply_operation(data)
            return web.json_response({
                "success": True,
                "document": document
            })
        except Exception as e:
            logger.error(f"Error applying operation: {e}")
            return web.json_response({
                "error": str(e)
            }, status=500)
            
    async def handle_network_topology(self, request):
        """Handle network topology request."""
        return web.json_response({
            "planets": self.planets,
            "nodes": self.nodes
        })
        
    async def handle_network_latency(self, request):
        """Handle network latency request."""
        return web.json_response({
            "planets": self.planets,
            "latencies": self.latencies
        })
        
    async def start(self):
        """Start the dashboard server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        self.running = True
        logger.info(f"Dashboard server running at http://{self.host}:{self.port}")
        
    async def stop(self):
        """Stop the dashboard server."""
        if self.running:
            await self.runner.cleanup()
            self.running = False
            logger.info("Dashboard server stopped")
            
    async def run(self):
        """Run the dashboard server."""
        await self.start()
        # Simulate network changes for visualization
        asyncio.create_task(self.simulate_network())
        
        try:
            while self.running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self.stop()
            
    async def simulate_network(self):
        """Periodically simulate changes in the network for visualization."""
        while self.running:
            # Update some node statuses randomly
            import random
            for node in self.nodes:
                if random.random() < 0.1:  # 10% chance to change status
                    node["online"] = not node["online"]
                    
            # Sleep for a random interval (3-8 seconds)
            await asyncio.sleep(random.uniform(3, 8))

async def main():
    """Main entry point."""
    port = int(os.environ.get('PORT', 4000))
    dashboard = ReliableDashboard(port=port)
    logger.info(f"Starting dashboard server on port {port}...")
    await dashboard.run()

if __name__ == "__main__":
    asyncio.run(main())