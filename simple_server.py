"""
Simple Server for Distributed OT Network

This script launches a simplified version of the OT network with only one node,
making it easier to visualize and interact with the system through a web browser.
"""

import asyncio
import logging
import os
import uuid
from typing import Dict, Any, Optional

from config import get_config
from core.ot_engine import InterPlanetaryOTEngine, Operation
from network.planetary_node import PlanetaryNode
from api.server import APIServer
from api.sin_integration import SINClient, SINIntegration

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleDocumentManager:
    """A simplified document manager for demonstration purposes."""
    
    def __init__(self):
        self.document = ""
        self.history = []
    
    def apply_operation(self, operation: Dict[str, Any]) -> str:
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
                length = 1  # Default to deleting one character
                if position + length <= len(self.document):
                    deleted = self.document[position:position+length]
                    self.document = self.document[:position] + self.document[position+length:]
                    self.history.append({
                        'type': 'delete',
                        'position': position,
                        'content': deleted
                    })
        
        return self.document

async def setup_web_server(port: int = 5000) -> None:
    """Set up and run the web server with a simple OT node."""
    # Create a simple document manager
    doc_manager = SimpleDocumentManager()
    
    # Initial document content
    doc_manager.document = "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transformation.\nYou can edit this text and see how changes are synchronized across the network."
    
    # Create a planetary node with a simplified setup
    node_id = f"earth-node-{str(uuid.uuid4())[:8]}"
    
    # Initialize OT engine
    ot_engine = InterPlanetaryOTEngine(planet_id="earth")
    
    # Create node
    node = PlanetaryNode(
        node_id=node_id,
        planet_id="earth",
        port=6000
    )
    node.ot_engine = ot_engine  # Set the OT engine
    
    # Create SIN client and integration
    sin_client = SINClient()
    sin_integration = SINIntegration(
        ot_engine=ot_engine,
        sin_client=sin_client
    )
    
    # Create API server with anonymous access enabled
    api_server = APIServer(
        node=node,
        host="0.0.0.0",
        port=port,
        cors_origins=["*"],
        allow_anonymous=True
    )
    
    # Override methods to work with our simple document manager
    async def get_document_state():
        return doc_manager.document
    
    node.get_document_state = get_document_state
    
    # Patch the SIN integration to work without actual SIN services
    async def create_document(content, metadata):
        doc_manager.document = content
        return "main"
    
    async def authenticate_user(username, password):
        return "anonymous-token"
    
    sin_integration.create_document = create_document
    sin_integration.authenticate_user = authenticate_user
    sin_integration.documents = {"main": {"content": doc_manager.document}}
    
    # Override document operations to use our simple manager
    original_handle_apply_operation = api_server.handle_apply_operation
    
    async def handle_apply_operation(request):
        try:
            # Extract operation details
            data = await request.json()
            
            # Apply operation to our simple document manager
            new_content = doc_manager.apply_operation(data)
            
            # Update the OT engine's document state
            ot_engine.document = new_content
            
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
    
    # Create a simplified document handler
    async def handle_get_document(request):
        document_id = request.match_info.get('document_id', 'main')
        
        try:
            return web.json_response({
                "success": True,
                "content": doc_manager.document
            })
        except Exception as e:
            logger.error(f"Error getting document: {e}")
            return web.json_response({
                "error": f"Failed to get document: {str(e)}"
            }, status=500)
    
    # Override the methods
    api_server.handle_apply_operation = handle_apply_operation
    api_server.handle_get_document = handle_get_document
    
    # Also provide network topology information
    async def handle_network_topology(request):
        return web.json_response({
            "planets": ["earth", "mars", "jupiter"],
            "nodes": [
                {"node_id": node_id, "planet_id": "earth", "online": True},
                {"node_id": "mars-node-1", "planet_id": "mars", "online": True},
                {"node_id": "mars-node-2", "planet_id": "mars", "online": True},
                {"node_id": "jupiter-node-1", "planet_id": "jupiter", "online": True}
            ]
        })
    
    async def handle_network_latency(request):
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
    
    api_server.handle_network_topology = handle_network_topology
    api_server.handle_network_latency = handle_network_latency
    
    # Start the server
    try:
        logger.info(f"Starting simplified OT network with node {node_id}")
        node.start()
        await sin_integration.start()
        await api_server.start()
        
        logger.info(f"Web server running at http://0.0.0.0:{port}")
        logger.info("Press Ctrl+C to stop")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
    finally:
        await api_server.stop()
        await sin_integration.stop()
        node.stop()
        
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    # Add necessary imports inside if block to avoid circular imports
    from aiohttp import web
    
    # Run the server
    asyncio.run(setup_web_server(port=5500))