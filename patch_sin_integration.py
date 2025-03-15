"""
SIN Integration Patch

This script creates a mock SIN API server running on localhost,
which the Operational Transform network can use instead of the real SIN API.
"""

import asyncio
import logging
import json
from aiohttp import web
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MockSINServer:
    """A mock SIN API server for testing."""
    
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        self.documents = {
            "main": {
                "id": "main",
                "content": "Welcome to the Distributed OT Network!\n\nThis is a collaborative text editor powered by Operational Transformation.",
                "version": 1,
                "created_at": "2025-03-14T00:00:00Z",
                "updated_at": "2025-03-14T00:00:00Z"
            }
        }
        
    def setup_routes(self):
        """Set up routes for the mock API."""
        self.app.router.add_get('/documents/{document_id}', self.handle_get_document)
        self.app.router.add_patch('/documents/{document_id}', self.handle_update_document)
        self.app.router.add_post('/documents', self.handle_create_document)
        
    async def handle_get_document(self, request):
        """Handle GET document request."""
        document_id = request.match_info.get('document_id')
        if document_id in self.documents:
            return web.json_response(self.documents[document_id])
        return web.json_response({"error": "Document not found"}, status=404)
        
    async def handle_update_document(self, request):
        """Handle PATCH document request."""
        document_id = request.match_info.get('document_id')
        if document_id not in self.documents:
            return web.json_response({"error": "Document not found"}, status=404)
            
        data = await request.json()
        document = self.documents[document_id]
        
        if 'content' in data:
            document['content'] = data['content']
            
        document['version'] += 1
        document['updated_at'] = "2025-03-14T00:30:00Z"  # Fake update time
        
        return web.json_response(document)
        
    async def handle_create_document(self, request):
        """Handle POST document request."""
        data = await request.json()
        document_id = f"doc-{len(self.documents) + 1}"
        
        document = {
            "id": document_id,
            "content": data.get('content', ''),
            "version": 1,
            "created_at": "2025-03-14T00:30:00Z",  # Fake creation time
            "updated_at": "2025-03-14T00:30:00Z"
        }
        
        self.documents[document_id] = document
        return web.json_response(document, status=201)
        
    async def start(self):
        """Start the mock API server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        self.site = web.TCPSite(runner, self.host, self.port)
        await self.site.start()
        logger.info(f"Mock SIN API server running at http://{self.host}:{self.port}")
        
        # Set environment variable for the real SIN client to use
        os.environ["SIN_API_ENDPOINT"] = f"http://{self.host}:{self.port}"
        os.environ["SIN_API_KEY"] = "mock-api-key"
        
        return runner
        
async def main():
    """Main entry point."""
    server = MockSINServer()
    runner = await server.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await runner.cleanup()
        logger.info("Mock SIN API server stopped")

if __name__ == "__main__":
    asyncio.run(main())