"""
API Server Module

This module implements the API server for the distributed OT network,
providing endpoints for client applications to interact with the network.

Key components:
- HTTP API endpoints
- WebSocket for real-time updates
- Authentication and authorization
- Client-side integration
"""

import os
import json
import time
import uuid
import logging
import asyncio
import hashlib
import hmac
import base64
import pathlib
from typing import Dict, List, Any, Optional, Set, Union

import aiohttp
from aiohttp import web
import aiohttp_cors
import jinja2
import aiohttp_jinja2

from core.ot_engine import Operation, InterPlanetaryOTEngine
from network.planetary_node import PlanetaryNode
from api.sin_integration import SINIntegration, SINClient, SINDocumentSession
from crypto.zkp import ZKPInterplanetaryAuth, ZKRangeProof

logger = logging.getLogger(__name__)

class APIServer:
    """API server for the distributed OT network."""
    
    def __init__(self,
                 node: PlanetaryNode,
                 host: str = "0.0.0.0",
                 port: int = 5000,
                 cors_origins: List[str] = None,
                 allow_anonymous: bool = False):
        """
        Initialize the API server.
        
        Args:
            node: Planetary node instance
            host: Host to bind to
            port: Port to listen on
            cors_origins: List of allowed CORS origins
            allow_anonymous: Whether to allow anonymous access
        """
        self.node = node
        self.host = host
        self.port = port
        self.cors_origins = cors_origins or ["*"]
        self.allow_anonymous = allow_anonymous
        
        # Web application
        self.app = web.Application()
        
        # Setup Jinja2 templates
        templates_path = pathlib.Path(__file__).parent / "templates"
        loader = jinja2.FileSystemLoader(str(templates_path))
        aiohttp_jinja2.setup(self.app, loader=loader)
        
        # Active WebSocket connections
        self.ws_connections: Dict[str, Dict[str, web.WebSocketResponse]] = {}
        
        # Active document sessions
        self.document_sessions: Dict[str, Dict[str, SINDocumentSession]] = {}
        
        # SIN integration
        self.sin_client = SINClient()
        self.sin_integration = SINIntegration(self.node.ot_engine, self.sin_client)
        
        # Authentication system
        self.auth = ZKPInterplanetaryAuth()
        
        # API tokens: token -> user_id
        self.api_tokens: Dict[str, str] = {}
        
        # Setup routes
        self._setup_routes()
        
    def _setup_routes(self) -> None:
        """Setup API routes."""
        # API routes
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/status', self.handle_status)
        
        # Authentication routes
        self.app.router.add_post('/auth/register', self.handle_register)
        self.app.router.add_post('/auth/login', self.handle_login)
        self.app.router.add_post('/auth/logout', self.handle_logout)
        self.app.router.add_post('/auth/challenge', self.handle_challenge)
        self.app.router.add_post('/auth/verify', self.handle_verify)
        
        # Document routes
        self.app.router.add_get('/documents', self.handle_list_documents)
        self.app.router.add_post('/documents', self.handle_create_document)
        self.app.router.add_get('/documents/{document_id}', self.handle_get_document)
        self.app.router.add_post('/documents/{document_id}/operations', self.handle_apply_operation)
        
        # WebSocket route
        self.app.router.add_get('/ws', self.handle_websocket)
        
        # Network routes
        self.app.router.add_get('/network/topology', self.handle_network_topology)
        self.app.router.add_get('/network/latency', self.handle_network_latency)
        
        # Setup CORS
        cors = aiohttp_cors.setup(self.app, defaults={
            origin: aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*"
            ) for origin in self.cors_origins
        })
        
        # Apply CORS to routes
        for route in list(self.app.router.routes()):
            cors.add(route)
            
    async def start(self) -> None:
        """Start the API server."""
        # Start SIN integration
        await self.sin_integration.start()
        
        # Create runner
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        # Create site
        site = web.TCPSite(runner, self.host, self.port)
        
        # Start site
        await site.start()
        
        logger.info(f"API server started on http://{self.host}:{self.port}")
        
    async def stop(self) -> None:
        """Stop the API server."""
        # Stop SIN integration
        await self.sin_integration.stop()
        
        # Close WebSocket connections
        for user_id, connections in self.ws_connections.items():
            for ws in connections.values():
                await ws.close(code=1000, message=b"Server shutdown")
                
        # Shutdown app
        await self.app.shutdown()
        
    async def authenticate_request(self, request: web.Request) -> Optional[str]:
        """
        Authenticate a request.
        
        Args:
            request: HTTP request
            
        Returns:
            User ID if authenticated, None otherwise
        """
        # Check for API token
        auth_header = request.headers.get('Authorization', '')
        
        if auth_header.startswith('Bearer '):
            token = auth_header[7:].strip()
            
            if token in self.api_tokens:
                return self.api_tokens[token]
                
        # Allow anonymous access if configured
        if self.allow_anonymous:
            return "anonymous"
            
        return None
        
    async def handle_index(self, request: web.Request) -> web.Response:
        """Handle index route."""
        # For API clients that expect JSON
        if request.headers.get('Accept') == 'application/json':
            return web.json_response({
                "name": "Distributed OT Network API",
                "version": "1.0.0",
                "status": "running",
                "planet": self.node.planet_id
            })
        
        # For browsers, serve the dashboard HTML
        try:
            with open(os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html'), 'r') as f:
                html_content = f.read()
            return web.Response(text=html_content, content_type='text/html')
        except Exception as e:
            logger.error(f"Error serving dashboard: {e}")
            return web.Response(text=f"""
            <html>
                <head><title>Distributed OT Network</title></head>
                <body>
                    <h1>Distributed OT Network</h1>
                    <p>Planet: {self.node.planet_id}</p>
                    <p>Node: {self.node.node_id}</p>
                    <p>Status: Running</p>
                    <p style="color: red;">Error loading dashboard: {str(e)}</p>
                </body>
            </html>
            """, content_type='text/html')
        
    async def handle_status(self, request: web.Request) -> web.Response:
        """Handle status route."""
        # Get node status
        document = await self.node.get_document_state()
        
        return web.json_response({
            "status": "running",
            "node_id": self.node.node_id,
            "planet_id": self.node.planet_id,
            "document_length": len(document),
            "connected_nodes": len(self.node.connected_nodes)
        })
        
    async def handle_register(self, request: web.Request) -> web.Response:
        """Handle user registration."""
        try:
            data = await request.json()
            
            username = data.get('username')
            password = data.get('password')
            
            if not username or not password:
                return web.json_response({
                    "error": "Missing username or password"
                }, status=400)
                
            # Register user
            self.auth.register_user(username, password)
            
            return web.json_response({
                "success": True,
                "message": "User registered successfully"
            })
            
        except Exception as e:
            logger.error(f"Error during registration: {e}")
            return web.json_response({
                "error": "Registration failed",
                "details": str(e)
            }, status=500)
            
    async def handle_login(self, request: web.Request) -> web.Response:
        """Handle user login."""
        try:
            data = await request.json()
            
            username = data.get('username')
            password = data.get('password')
            
            if not username or not password:
                return web.json_response({
                    "error": "Missing username or password"
                }, status=400)
                
            # Authenticate with SIN
            token = await self.sin_integration.authenticate_user(username, password)
            
            if token:
                # Store API token
                self.api_tokens[token] = username
                
                return web.json_response({
                    "success": True,
                    "token": token
                })
            else:
                return web.json_response({
                    "error": "Authentication failed"
                }, status=401)
                
        except Exception as e:
            logger.error(f"Error during login: {e}")
            return web.json_response({
                "error": "Login failed",
                "details": str(e)
            }, status=500)
            
    async def handle_logout(self, request: web.Request) -> web.Response:
        """Handle user logout."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        # Remove token
        token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
        
        if token in self.api_tokens:
            del self.api_tokens[token]
            
        return web.json_response({
            "success": True,
            "message": "Logged out successfully"
        })
        
    async def handle_challenge(self, request: web.Request) -> web.Response:
        """Handle authentication challenge request."""
        try:
            data = await request.json()
            
            username = data.get('username')
            
            if not username:
                return web.json_response({
                    "error": "Missing username"
                }, status=400)
                
            # Generate challenge
            challenge = self.auth.generate_long_term_challenge(username)
            
            if challenge:
                return web.json_response({
                    "success": True,
                    "challenge": challenge
                })
            else:
                return web.json_response({
                    "error": "User not found"
                }, status=404)
                
        except Exception as e:
            logger.error(f"Error generating challenge: {e}")
            return web.json_response({
                "error": "Challenge generation failed",
                "details": str(e)
            }, status=500)
            
    async def handle_verify(self, request: web.Request) -> web.Response:
        """Handle authentication verification."""
        try:
            data = await request.json()
            
            response_data = data.get('response')
            
            if not response_data:
                return web.json_response({
                    "error": "Missing response data"
                }, status=400)
                
            # Verify response
            if self.auth.verify_long_term_proof(response_data):
                # Generate token
                token = base64.b64encode(os.urandom(32)).decode('ascii')
                self.api_tokens[token] = response_data["username"]
                
                return web.json_response({
                    "success": True,
                    "token": token
                })
            else:
                return web.json_response({
                    "error": "Verification failed"
                }, status=401)
                
        except Exception as e:
            logger.error(f"Error verifying challenge: {e}")
            return web.json_response({
                "error": "Verification failed",
                "details": str(e)
            }, status=500)
            
    async def handle_list_documents(self, request: web.Request) -> web.Response:
        """Handle document listing."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        try:
            # Get documents from SIN
            async with self.sin_client as client:
                # In a real implementation, we would filter by user
                # For now, return the list of tracked documents
                documents = list(self.sin_integration.documents.keys())
                
            return web.json_response({
                "success": True,
                "documents": documents
            })
            
        except Exception as e:
            logger.error(f"Error listing documents: {e}")
            return web.json_response({
                "error": "Failed to list documents",
                "details": str(e)
            }, status=500)
            
    async def handle_create_document(self, request: web.Request) -> web.Response:
        """Handle document creation."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        try:
            data = await request.json()
            
            content = data.get('content', '')
            metadata = data.get('metadata', {})
            
            # Add user as owner
            metadata['owner'] = user_id
            
            # Create document
            document_id = await self.sin_integration.create_document(content, metadata)
            
            return web.json_response({
                "success": True,
                "document_id": document_id
            })
            
        except Exception as e:
            logger.error(f"Error creating document: {e}")
            return web.json_response({
                "error": "Failed to create document",
                "details": str(e)
            }, status=500)
            
    async def handle_get_document(self, request: web.Request) -> web.Response:
        """Handle document retrieval."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        document_id = request.match_info['document_id']
        
        try:
            # Get document from SIN
            async with self.sin_client as client:
                sin_doc = await client.get_document(document_id)
                
            # Check authorization (simplified - would normally use SIN's auth)
            if user_id != sin_doc.get('owner') and user_id != "anonymous":
                return web.json_response({
                    "error": "Access denied"
                }, status=403)
                
            # Ensure document is tracked
            if document_id not in self.sin_integration.documents:
                await self.sin_integration.track_document(document_id)
                
            # Get current content from OT engine
            content = self.sin_integration.ot_engine.document
            
            return web.json_response({
                "success": True,
                "document": {
                    "id": document_id,
                    "content": content,
                    "metadata": {k: v for k, v in sin_doc.items() if k != 'content'}
                }
            })
            
        except Exception as e:
            logger.error(f"Error getting document: {e}")
            return web.json_response({
                "error": "Failed to get document",
                "details": str(e)
            }, status=500)
            
    async def handle_apply_operation(self, request: web.Request) -> web.Response:
        """Handle operation application."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        document_id = request.match_info['document_id']
        
        try:
            data = await request.json()
            
            op_type = data.get('op_type')
            position = data.get('position')
            content = data.get('content')
            
            if op_type not in ('insert', 'delete') or position is None:
                return web.json_response({
                    "error": "Invalid operation"
                }, status=400)
                
            # Get or create document session
            session_key = f"{user_id}:{document_id}"
            
            if document_id not in self.document_sessions:
                self.document_sessions[document_id] = {}
                
            if session_key not in self.document_sessions[document_id]:
                # Create and authorize session
                session = SINDocumentSession(document_id, user_id, self.sin_integration)
                
                # Generate auth proof
                auth_proof = self.sin_integration.generate_auth_proof(user_id, document_id)
                
                if not auth_proof or not await session.authorize(auth_proof):
                    return web.json_response({
                        "error": "Authorization failed"
                    }, status=403)
                    
                self.document_sessions[document_id][session_key] = session
                
            session = self.document_sessions[document_id][session_key]
            
            # Create and apply operation
            operation = Operation(
                op_type=op_type,
                position=position,
                content=content,
                client_id=user_id
            )
            
            success = await session.apply_operation(operation)
            
            if success:
                # Broadcast update to WebSocket clients
                await self._broadcast_operation(document_id, operation)
                
                return web.json_response({
                    "success": True
                })
            else:
                return web.json_response({
                    "error": "Failed to apply operation"
                }, status=500)
                
        except Exception as e:
            logger.error(f"Error applying operation: {e}")
            return web.json_response({
                "error": "Failed to apply operation",
                "details": str(e)
            }, status=500)
            
    async def handle_websocket(self, request: web.Request) -> web.WebSocketResponse:
        """Handle WebSocket connections."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # Generate connection ID
        connection_id = str(uuid.uuid4())
        
        # Store connection
        if user_id not in self.ws_connections:
            self.ws_connections[user_id] = {}
            
        self.ws_connections[user_id][connection_id] = ws
        
        try:
            # Send initial message
            await ws.send_json({
                "type": "connected",
                "user_id": user_id,
                "connection_id": connection_id
            })
            
            # Process messages
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        
                        message_type = data.get('type')
                        
                        if message_type == 'subscribe':
                            # Subscribe to document updates
                            document_id = data.get('document_id')
                            
                            if document_id:
                                # Get or create document session
                                session_key = f"{user_id}:{document_id}"
                                
                                if document_id not in self.document_sessions:
                                    self.document_sessions[document_id] = {}
                                    
                                if session_key not in self.document_sessions[document_id]:
                                    # Create and authorize session
                                    session = SINDocumentSession(document_id, user_id, self.sin_integration)
                                    
                                    # Generate auth proof
                                    auth_proof = self.sin_integration.generate_auth_proof(user_id, document_id)
                                    
                                    if not auth_proof or not await session.authorize(auth_proof):
                                        await ws.send_json({
                                            "type": "error",
                                            "error": "Authorization failed",
                                            "document_id": document_id
                                        })
                                        continue
                                        
                                    self.document_sessions[document_id][session_key] = session
                                    
                                # Send current content
                                session = self.document_sessions[document_id][session_key]
                                content = await session.get_content()
                                
                                await ws.send_json({
                                    "type": "document_state",
                                    "document_id": document_id,
                                    "content": content
                                })
                                
                        elif message_type == 'operation':
                            # Apply operation
                            document_id = data.get('document_id')
                            operation_data = data.get('operation')
                            
                            if document_id and operation_data:
                                # Get document session
                                session_key = f"{user_id}:{document_id}"
                                
                                if document_id in self.document_sessions and session_key in self.document_sessions[document_id]:
                                    session = self.document_sessions[document_id][session_key]
                                    
                                    # Create operation
                                    operation = Operation(
                                        op_type=operation_data.get('op_type'),
                                        position=operation_data.get('position'),
                                        content=operation_data.get('content'),
                                        client_id=user_id
                                    )
                                    
                                    # Apply operation
                                    success = await session.apply_operation(operation)
                                    
                                    if success:
                                        # Broadcast update
                                        await self._broadcast_operation(document_id, operation)
                                        
                                        await ws.send_json({
                                            "type": "operation_ack",
                                            "document_id": document_id,
                                            "operation_id": operation.operation_id,
                                            "status": "success"
                                        })
                                    else:
                                        await ws.send_json({
                                            "type": "operation_ack",
                                            "document_id": document_id,
                                            "operation_id": operation.operation_id,
                                            "status": "error",
                                            "error": "Failed to apply operation"
                                        })
                                else:
                                    await ws.send_json({
                                        "type": "error",
                                        "error": "Not subscribed to document",
                                        "document_id": document_id
                                    })
                                    
                    except json.JSONDecodeError:
                        await ws.send_json({
                            "type": "error",
                            "error": "Invalid JSON"
                        })
                    except Exception as e:
                        logger.error(f"Error processing WebSocket message: {e}")
                        await ws.send_json({
                            "type": "error",
                            "error": str(e)
                        })
                        
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws.exception()}")
                    break
                    
        finally:
            # Remove connection
            if user_id in self.ws_connections and connection_id in self.ws_connections[user_id]:
                del self.ws_connections[user_id][connection_id]
                
                if not self.ws_connections[user_id]:
                    del self.ws_connections[user_id]
                    
        return ws
        
    async def handle_network_topology(self, request: web.Request) -> web.Response:
        """Handle network topology request."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        try:
            # Get network topology
            topology = await self.node.get_network_topology()
            
            return web.json_response({
                "success": True,
                "topology": topology
            })
            
        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            return web.json_response({
                "error": "Failed to get network topology",
                "details": str(e)
            }, status=500)
            
    async def handle_network_latency(self, request: web.Request) -> web.Response:
        """Handle network latency request."""
        user_id = await self.authenticate_request(request)
        
        if not user_id:
            return web.json_response({
                "error": "Unauthorized"
            }, status=401)
            
        try:
            # Get inter-planetary latency
            latency = await self.node.get_interplanetary_latency()
            
            return web.json_response({
                "success": True,
                "latency": latency
            })
            
        except Exception as e:
            logger.error(f"Error getting network latency: {e}")
            return web.json_response({
                "error": "Failed to get network latency",
                "details": str(e)
            }, status=500)
            
    async def _broadcast_operation(self, document_id: str, operation: Operation) -> None:
        """
        Broadcast an operation to WebSocket clients.
        
        Args:
            document_id: ID of the document
            operation: Operation to broadcast
        """
        if document_id not in self.document_sessions:
            return
            
        # Create message
        message = {
            "type": "operation",
            "document_id": document_id,
            "operation": operation.to_dict()
        }
        
        # Send to all connected users
        for user_id, sessions in self.document_sessions[document_id].items():
            # Extract user ID from session key
            user_id = user_id.split(":")[0]
            
            if user_id in self.ws_connections:
                for ws in self.ws_connections[user_id].values():
                    try:
                        await ws.send_json(message)
                    except Exception as e:
                        logger.error(f"Error sending WebSocket message: {e}")
