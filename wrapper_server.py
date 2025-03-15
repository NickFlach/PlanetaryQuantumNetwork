"""
Wrapper Server for Distributed OT Network

This is a simple wrapper around our application that binds to port 5000 and
forwards requests to our application server on port 5500.
"""

import logging
import asyncio
from aiohttp import web, ClientSession, TCPConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleProxy:
    def __init__(self, target_url='http://localhost:5500', bind_port=5000):
        self.target_url = target_url
        self.bind_port = bind_port
        self.app = web.Application()
        self.setup_routes()
        self.client_session = None
    
    def setup_routes(self):
        self.app.router.add_get('/', self.handle_request)
        self.app.router.add_get('/{path:.*}', self.handle_request)
        self.app.router.add_post('/{path:.*}', self.handle_request)
        self.app.router.add_put('/{path:.*}', self.handle_request)
        self.app.router.add_delete('/{path:.*}', self.handle_request)
    
    async def handle_request(self, request):
        path = request.match_info.get('path', '')
        target_url = f"{self.target_url}/{path}"
        
        # Add query parameters
        if request.query_string:
            target_url = f"{target_url}?{request.query_string}"
        
        method = request.method
        headers = {k: v for k, v in request.headers.items() if k.lower() not in ('host',)}
        
        if not self.client_session:
            self.client_session = ClientSession(connector=TCPConnector(limit=100))
        
        try:
            # Get request body for POST, PUT requests
            data = None
            if method in ('POST', 'PUT'):
                data = await request.read()
            
            logger.info(f"Forwarding {method} request to {target_url}")
            
            async with self.client_session.request(
                method, 
                target_url,
                headers=headers,
                data=data,
                allow_redirects=False
            ) as response:
                # Read response body
                body = await response.read()
                
                # Prepare the response with the same status, headers, and body
                resp = web.Response(
                    status=response.status,
                    body=body
                )
                
                # Copy headers from the target response
                for header_name, header_value in response.headers.items():
                    if header_name.lower() not in ('server', 'transfer-encoding'):
                        resp.headers[header_name] = header_value
                
                return resp
        except Exception as e:
            logger.error(f"Error forwarding request: {e}")
            return web.Response(
                status=502,
                text=f"Error forwarding request: {str(e)}"
            )
    
    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.bind_port)
        await site.start()
        logger.info(f"Proxy server running at http://0.0.0.0:{self.bind_port}")
        logger.info(f"Forwarding to {self.target_url}")
        return runner
    
    async def stop(self):
        if self.client_session:
            await self.client_session.close()

async def main():
    import subprocess
    import sys
    
    # Start the actual server in a separate process
    server_process = subprocess.Popen([sys.executable, 'simple_server.py'])
    
    # Wait a moment for the server to start
    await asyncio.sleep(2)
    
    # Start the proxy
    proxy = SimpleProxy()
    runner = await proxy.start()
    
    try:
        # Keep the server running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Clean up
        await proxy.stop()
        await runner.cleanup()
        
        # Terminate the server process
        server_process.terminate()
        server_process.wait()
        
        logger.info("Proxy server shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())