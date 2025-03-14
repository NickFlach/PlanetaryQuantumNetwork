"""
Communication Module

This module implements the communication layer for the distributed OT network,
handling message passing between nodes across high-latency connections.

Key components:
- Message definition and serialization
- Asynchronous messaging
- Connection management
- Error handling and retries
"""

import asyncio
import enum
import json
import logging
import time
import uuid
from typing import Dict, List, Tuple, Any, Optional, Union, Callable, Awaitable

logger = logging.getLogger(__name__)

class MessageType(enum.Enum):
    """Types of messages that can be exchanged between nodes."""
    HANDSHAKE = "handshake"  # Initial connection handshake
    DISCONNECT = "disconnect"  # Disconnect notification
    KEY_EXCHANGE = "key_exchange"  # Quantum-resistant key exchange
    KEY_ENCAPSULATION = "key_encapsulation"  # Key encapsulation
    KEY_CONFIRMATION = "key_confirmation"  # Key exchange confirmation
    ENCRYPTED_DATA = "encrypted_data"  # Encrypted application data
    PING = "ping"  # Connectivity check
    PONG = "pong"  # Ping response
    ERROR = "error"  # Error notification


class Message:
    """Represents a message in the inter-planetary OT network."""
    
    def __init__(self, 
                 message_id: str,
                 message_type: MessageType,
                 source_id: str,
                 target_id: str,
                 data: Any,
                 timestamp: float = None):
        """
        Initialize a message.
        
        Args:
            message_id: Unique identifier for the message
            message_type: Type of message
            source_id: ID of the source node
            target_id: ID of the target node
            data: Message payload
            timestamp: Time when the message was created
        """
        self.message_id = message_id
        self.message_type = message_type
        self.source_id = source_id
        self.target_id = target_id
        self.data = data
        self.timestamp = timestamp or time.time()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert the message to a dictionary."""
        # Handle binary data by base64 encoding it
        data = self.data
        if isinstance(data, bytes):
            import base64
            data = {
                "__binary__": True,
                "data": base64.b64encode(data).decode('ascii')
            }
            
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "data": data,
            "timestamp": self.timestamp
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create a message from a dictionary."""
        # Handle binary data conversion
        message_data = data["data"]
        if isinstance(message_data, dict) and message_data.get("__binary__", False):
            import base64
            message_data = base64.b64decode(message_data["data"])
        
        return cls(
            message_id=data["message_id"],
            message_type=MessageType(data["message_type"]),
            source_id=data["source_id"],
            target_id=data["target_id"],
            data=message_data,
            timestamp=data["timestamp"]
        )
        
    def __str__(self) -> str:
        return f"Message({self.message_id}, {self.message_type}, {self.source_id} -> {self.target_id})"


class Connection:
    """Represents a connection to another node."""
    
    def __init__(self, node_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Initialize a connection.
        
        Args:
            node_id: ID of the connected node
            reader: Stream reader for incoming data
            writer: Stream writer for outgoing data
        """
        self.node_id = node_id
        self.reader = reader
        self.writer = writer
        self.address = writer.get_extra_info('peername')
        self.connected = True
        self.last_activity = time.time()
        
    async def send_message(self, message: Message) -> None:
        """
        Send a message over the connection.
        
        Args:
            message: Message to send
        """
        if not self.connected:
            raise ConnectionError(f"Connection to node {self.node_id} is closed")
            
        # Serialize the message with improved error handling
        try:
            message_dict = message.to_dict()
            # Use a more robust JSON serialization with explicit encoding
            message_data = json.dumps(message_dict, ensure_ascii=False, 
                                     default=str).encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to serialize message to {self.node_id}: {e}, message: {message}")
            self.connected = False
            raise ConnectionError(f"Serialization error: {e}")
            
        # Send the message length first, then the message
        message_length = len(message_data)
        length_bytes = message_length.to_bytes(4, byteorder='big')
        
        try:
            # Combine writes to avoid fragmentation issues
            combined_data = bytearray(length_bytes)
            combined_data.extend(message_data)
            
            self.writer.write(combined_data)
            
            # Use careful error handling for drain
            try:
                await asyncio.wait_for(self.writer.drain(), timeout=5.0)
                self.last_activity = time.time()
            except asyncio.TimeoutError:
                logger.warning(f"Timeout draining writer for node {self.node_id}")
                self.connected = False
                raise ConnectionError(f"Write timeout for node {self.node_id}")
                
        except (ConnectionError, BrokenPipeError, OSError) as e:
            self.connected = False
            raise ConnectionError(f"Failed to send message to node {self.node_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending message to {self.node_id}: {e}")
            self.connected = False
            raise ConnectionError(f"Failed to send message to node {self.node_id}: {e}")
            
    async def receive_message(self) -> Optional[Message]:
        """
        Receive a message from the connection.
        
        Returns:
            Received message, or None if the connection was closed
        """
        if not self.connected:
            return None
            
        try:
            # Use timeout for read operations to avoid hanging indefinitely
            try:
                # Read message length with timeout
                length_bytes = await asyncio.wait_for(
                    self.reader.readexactly(4), 
                    timeout=10.0
                )
                message_length = int.from_bytes(length_bytes, byteorder='big')
                
                # Sanity check for message size
                if message_length <= 0 or message_length > 10 * 1024 * 1024:  # 10MB max
                    logger.warning(f"Invalid message length: {message_length} from {self.node_id}")
                    self.connected = False
                    return None
                
                # Read message data with timeout
                message_data = await asyncio.wait_for(
                    self.reader.readexactly(message_length),
                    timeout=30.0  # Extended timeout for larger messages or network delays
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout reading message from {self.node_id}")
                # Don't immediately mark connection as disconnected
                # Instead, we'll try a ping to see if connection is still active
                try:
                    # Try to write a small ping packet to test connection
                    self.writer.write(b'\x00\x00\x00\x01\x00')  # Minimal ping
                    await self.writer.drain()
                    logger.debug(f"Sent probe ping to {self.node_id}")
                    
                    # Try to read response
                    probe_response = await asyncio.wait_for(self.reader.read(1), timeout=2.0)
                    if probe_response:
                        logger.info(f"Connection to {self.node_id} is still responsive despite timeout")
                        # Connection seems alive but message reading timed out
                        # This is partial fail, but we'll allow another attempt
                        return None
                except Exception:
                    # If ping fails, connection is really gone
                    logger.warning(f"Connection to {self.node_id} confirmed dead after probe")
                    self.connected = False
                    return None
                
                # If we get here, ping didn't work
                self.connected = False
                return None
            
            # Parse message with improved error handling
            try:
                # Using a more robust decode process with fallback
                try:
                    # First try standard UTF-8 decode
                    message_str = message_data.decode('utf-8')
                except UnicodeDecodeError:
                    # If that fails, use replacement for invalid chars
                    logger.warning(f"UTF-8 decode failed for message from {self.node_id}, using replacement mode")
                    message_str = message_data.decode('utf-8', errors='replace')
                
                try:
                    # Try to parse the JSON with strict mode first
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError as e:
                    # Log the error and the problematic JSON
                    logger.error(f"JSON decode error from {self.node_id}: {e}")
                    logger.debug(f"Problematic JSON (truncated): {message_str[:200]}")
                    # Re-raise to be caught by the outer exception handler
                    raise
                
                # Create Message object from the dict
                message = Message.from_dict(message_dict)
                
                self.last_activity = time.time()
                return message
                
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(f"Failed to decode message from node {self.node_id}: {e}")
                logger.debug(f"Raw message data (first 100 bytes): {message_data[:100]}")
                return None
            
        except asyncio.IncompleteReadError:
            # Connection closed
            logger.warning(f"Connection closed by peer while reading from {self.node_id}")
            self.connected = False
            return None
        except (ConnectionError, OSError) as e:
            logger.warning(f"Connection error while reading from {self.node_id}: {e}")
            self.connected = False
            return None
        except Exception as e:
            logger.error(f"Unexpected error receiving message from {self.node_id}: {e}")
            self.connected = False
            return None
            
    async def close(self) -> None:
        """Close the connection."""
        if not self.connected:
            return
            
        self.connected = False
        
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class MessageHandler:
    """Handles message passing between nodes."""
    
    def __init__(self, message_callback: Callable[[Message], Awaitable[None]],
                 node_id: str = None, bind_address: str = "0.0.0.0", port: int = 8000):
        """
        Initialize the message handler.
        
        Args:
            message_callback: Callback function for received messages
            node_id: ID of the local node
            bind_address: Address to bind the server to
            port: Port to listen on
        """
        self.node_id = node_id or str(uuid.uuid4())
        self.bind_address = bind_address
        self.port = port
        self.message_callback = message_callback
        
        # Active connections
        self.connections: Dict[str, Connection] = {}
        
        # Server
        self.server = None
        
        # Message queues for waiting for specific messages
        self.message_queues: Dict[str, asyncio.Queue] = {}
        
        # Running flag
        self.running = False
        
        # Event loop
        self.loop = asyncio.get_event_loop()
        
    async def start_server(self) -> None:
        """Start the server to accept incoming connections."""
        if self.server:
            return
            
        # Create server
        self.server = await asyncio.start_server(
            self._handle_connection, self.bind_address, self.port)
            
        # Start serving
        addr = self.server.sockets[0].getsockname()
        logger.info(f"Message handler server started on {addr}")
        
        # Start background tasks
        self.loop.create_task(self._heartbeat_task())
        
        async with self.server:
            await self.server.serve_forever()
            
    def start(self) -> None:
        """Start the message handler."""
        if self.running:
            return
            
        self.running = True
        
        # Start the server in a background task
        self.loop.create_task(self.start_server())
        
    def stop(self) -> None:
        """Stop the message handler."""
        if not self.running:
            return
            
        self.running = False
        
        # Close server
        if self.server:
            self.server.close()
            
        # Close all connections
        for connection in list(self.connections.values()):
            self.loop.create_task(connection.close())
            
    async def _handle_connection(self, reader: asyncio.StreamReader, 
                                writer: asyncio.StreamWriter) -> None:
        """
        Handle a new connection.
        
        Args:
            reader: Stream reader for incoming data
            writer: Stream writer for outgoing data
        """
        # Create connection object (node_id will be set later)
        conn = Connection("unknown", reader, writer)
        peer_addr = conn.address
        
        logger.info(f"New connection from {peer_addr}")
        
        try:
            # Improved handshake reception with enhanced retry mechanism
            message = None
            retries = 5  # Increased from 3 to 5 for better reliability
            base_timeout = 5.0
            
            # Send a pre-handshake welcome message to confirm connection is viable
            try:
                welcome = Message(
                    message_id=str(uuid.uuid4()),
                    message_type=MessageType.PING,
                    source_id=self.node_id,
                    target_id="unknown",
                    data={
                        "status": "welcome",
                        "server_time": time.time(),
                        "message": "Connection established, waiting for handshake"
                    }
                )
                await conn.send_message(welcome)
                logger.debug(f"Sent welcome message to {peer_addr}")
            except Exception as e:
                logger.warning(f"Failed to send welcome to {peer_addr}: {e}")
                # Continue anyway - the welcome is just an optimization
            
            # Enhanced handshake reception loop with better error handling
            for retry in range(retries):
                # Calculate timeout with exponential backoff
                timeout = base_timeout * (1.5 ** retry)
                
                try:
                    logger.info(f"Waiting for handshake from {peer_addr} (attempt {retry+1}/{retries}, timeout: {timeout:.1f}s)")
                    message = await asyncio.wait_for(conn.receive_message(), timeout=timeout)
                    
                    if not message:
                        logger.warning(f"Empty handshake message from {peer_addr} (attempt {retry+1}/{retries})")
                        # Send another welcome message as a prompt
                        try:
                            welcome.data["retry"] = retry
                            welcome.data["server_time"] = time.time()
                            await conn.send_message(welcome)
                        except Exception:
                            pass  # Ignore errors here
                        await asyncio.sleep(0.5)
                        continue
                    
                    # Validate message type
                    if message.message_type != MessageType.HANDSHAKE:
                        logger.warning(f"Received non-handshake message type from {peer_addr}: {message.message_type}")
                        
                        # If this is a PING, respond with PONG but keep waiting for handshake
                        if message.message_type == MessageType.PING:
                            try:
                                pong = Message(
                                    message_id=str(uuid.uuid4()),
                                    message_type=MessageType.PONG,
                                    source_id=self.node_id,
                                    target_id=message.source_id or "unknown",
                                    data={
                                        "echo": message.data,
                                        "server_time": time.time(),
                                        "message": "Still waiting for proper handshake"
                                    }
                                )
                                await conn.send_message(pong)
                            except Exception:
                                pass  # Ignore errors
                            await asyncio.sleep(0.5)
                            continue
                        
                        # For other message types, reject explicitly
                        rejection = Message(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.HANDSHAKE,
                            source_id=self.node_id,
                            target_id=message.source_id or "unknown",
                            data={
                                "status": "rejected",
                                "reason": f"Expected handshake, got {message.message_type}",
                                "server_time": time.time()
                            }
                        )
                        try:
                            await conn.send_message(rejection)
                        except Exception:
                            pass  # Ignore errors
                        await conn.close()
                        return
                    
                    # We have a valid handshake message, break out of the retry loop
                    logger.info(f"Received handshake message from {peer_addr} on attempt {retry+1}")
                    break
                    
                except asyncio.TimeoutError:
                    if retry < retries - 1:  # Don't log on the last retry
                        logger.warning(f"Handshake timeout ({timeout:.1f}s) from {peer_addr}, attempt {retry+1}/{retries}")
                        
                except ConnectionError as e:
                    logger.warning(f"Connection error during handshake from {peer_addr}: {e}")
                    await conn.close()
                    return
                    
                except Exception as e:
                    logger.warning(f"Error during handshake from {peer_addr}: {e}")
                    if retry >= retries - 1:  # Last retry
                        await conn.close()
                        return
            
            # Final validation check
            if not message:
                logger.warning(f"Handshake failed after {retries} attempts from {peer_addr}")
                await conn.close()
                return
                
            # Extract node ID and handshake information
            node_id = message.source_id
            handshake_id = message.data.get("handshake_id", "unknown")
            client_version = message.data.get("version", "unknown")
            node_type = message.data.get("node_type", "unknown")
            timestamp = message.data.get("timestamp", time.time())
            
            logger.info(f"Received handshake from node {node_id} (version={client_version}, type={node_type})")
            
            if node_id in self.connections:
                # Close existing connection to avoid duplicates
                logger.info(f"Replacing existing connection for node {node_id}")
                old_conn = self.connections[node_id]
                await old_conn.close()
                
            # Update connection with node ID
            conn.node_id = node_id
            self.connections[node_id] = conn
            
            logger.info(f"Handshake completed with node {node_id} at {peer_addr}")
            
            # Send enhanced handshake response with retry logic for reliability
            for attempt in range(3):  # Try up to 3 times
                try:
                    response = Message(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.HANDSHAKE,
                        source_id=self.node_id,
                        target_id=node_id,
                        data={
                            "status": "accepted",
                            "handshake_id": handshake_id,  # Echo back the handshake ID
                            "version": "1.0",
                            "node_type": "planetary",
                            "timestamp": time.time(),
                            "server_time": time.time(),
                            "time_diff": time.time() - timestamp,  # Help with time sync
                            "attempt": attempt + 1
                        }
                    )
                    await conn.send_message(response)
                    
                    # Introduce a small delay after sending the handshake response 
                    # to allow the client to process it before continuing
                    await asyncio.sleep(0.1)
                    
                    # Success - break out of retry loop
                    break
                    
                except Exception as e:
                    if attempt < 2:  # Only log warning for non-final attempts
                        logger.warning(f"Failed to send handshake response to {node_id} (attempt {attempt+1}/3): {e}")
                        await asyncio.sleep(0.2)  # Small delay before retry
                    else:
                        logger.error(f"Failed to send handshake response to {node_id} after 3 attempts: {e}")
            
            # Instead of immediate stability confirmation, slightly defer it and use a safer approach
            # This helps avoid the race condition that's causing broken pipes
            async def send_deferred_stability_confirmation():
                try:
                    # Wait a bit to let connection stabilize before sending additional messages
                    await asyncio.sleep(0.5)
                    
                    # Check if connection is still valid before sending
                    if node_id in self.connections and self.connections[node_id].connected:
                        stability_confirmation = Message(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.PING,
                            source_id=self.node_id,
                            target_id=node_id,
                            data={
                                "handshake_confirmed": True,
                                "timestamp": time.time(),
                                "message": "Connection established and stable"
                            }
                        )
                        
                        # Use a shorter timeout for this message to avoid blocking
                        try:
                            await asyncio.wait_for(
                                self.connections[node_id].send_message(stability_confirmation),
                                timeout=1.0
                            )
                            logger.debug(f"Sent deferred stability confirmation to {node_id}")
                        except (asyncio.TimeoutError, ConnectionError) as e:
                            logger.debug(f"Couldn't send stability confirmation to {node_id}: {e}")
                    else:
                        logger.debug(f"Skipping stability confirmation - connection to {node_id} no longer valid")
                except Exception as e:
                    logger.debug(f"Error in deferred stability confirmation for {node_id}: {e}")
                    # Ignore errors - this is just an optimization
            
            # Create the task but don't await it - let it run independently
            asyncio.create_task(send_deferred_stability_confirmation())
            
            # Process messages with improved error handling
            while conn.connected and self.running:
                try:
                    # Use a heartbeat-based timeout
                    message = await asyncio.wait_for(conn.receive_message(), timeout=60.0)
                    
                    if not message:
                        logger.info(f"Connection closed by node {node_id}")
                        break
                        
                    # Check if message is for us
                    if message.target_id != self.node_id:
                        logger.warning(f"Received message for {message.target_id} from {node_id}")
                        continue
                        
                    # Handle special message types
                    if message.message_type == MessageType.DISCONNECT:
                        logger.info(f"Received disconnect from node {node_id}")
                        # Acknowledge the disconnect
                        ack = Message(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.DISCONNECT,
                            source_id=self.node_id,
                            target_id=node_id,
                            data={"status": "acknowledged"}
                        )
                        try:
                            await conn.send_message(ack)
                        except Exception:
                            pass  # Ignore errors during disconnect acknowledgment
                        break
                        
                    elif message.message_type == MessageType.PING:
                        # Respond to ping with enhanced data
                        response = Message(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.PONG,
                            source_id=self.node_id,
                            target_id=node_id,
                            data={
                                "echo": message.data,
                                "server_time": time.time()
                            }
                        )
                        await conn.send_message(response)
                        continue
                        
                    # Pass to callback
                    await self._process_message(message)
                    
                except asyncio.TimeoutError:
                    # Connection is idle, send ping to check if it's still alive
                    try:
                        ping = Message(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.PING,
                            source_id=self.node_id,
                            target_id=node_id,
                            data={"timestamp": time.time()}
                        )
                        await conn.send_message(ping)
                    except ConnectionError:
                        logger.warning(f"Connection to node {node_id} is dead")
                        break
                        
                except ConnectionError as e:
                    logger.warning(f"Connection error with node {node_id}: {e}")
                    break
                    
                except Exception as e:
                    logger.error(f"Error processing message from node {node_id}: {e}")
                    # Continue processing despite errors
                
        except Exception as e:
            logger.error(f"Error handling connection from {peer_addr}: {e}")
            
        finally:
            # Clean up connection
            await conn.close()
            
            if conn.node_id in self.connections and self.connections[conn.node_id] is conn:
                del self.connections[conn.node_id]
                
            logger.info(f"Connection closed with {peer_addr}")
            
    async def _process_message(self, message: Message) -> None:
        """
        Process a received message.
        
        Args:
            message: Received message
        """
        # Check if someone is waiting for this message type from this source
        queue_key = f"{message.source_id}:{message.message_type.value}"
        
        if queue_key in self.message_queues:
            # Add to queue
            await self.message_queues[queue_key].put(message)
        else:
            # Pass to callback
            await self.message_callback(message)
            
    async def connect(self, node_id: str, address: str) -> None:
        """
        Connect to another node.
        
        Args:
            node_id: ID of the target node
            address: Address of the target node (host:port)
        """
        if node_id in self.connections:
            logger.info(f"Already connected to node {node_id}")
            return
            
        # Parse address
        host, port_str = address.split(":")
        port = int(port_str)
        
        # Set up retry parameters
        max_retries = 3
        retry_delay = 1.0  # seconds
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Connect
                logger.info(f"Attempting to connect to node {node_id} at {address} (attempt {retry_count+1}/{max_retries})")
                reader, writer = await asyncio.open_connection(host, port)
                
                # Create connection
                conn = Connection(node_id, reader, writer)
                self.connections[node_id] = conn
                
                logger.info(f"Connected to node {node_id} at {address}")
                
                # Send handshake with connection identifier
                handshake_id = str(uuid.uuid4())
                handshake = Message(
                    message_id=handshake_id,
                    message_type=MessageType.HANDSHAKE,
                    source_id=self.node_id,
                    target_id=node_id,
                    data={
                        "version": "1.0",
                        "handshake_id": handshake_id,
                        "node_type": "planetary",
                        "timestamp": time.time()
                    }
                )
                await conn.send_message(handshake)
                
                # Use improved handshake protocol with reliable wait and timeout strategy
                max_handshake_retries = 5  # Increased from 3 to 5
                handshake_retry_count = 0
                response = None
                base_timeout = 5.0
                
                while handshake_retry_count < max_handshake_retries and not response:
                    # Calculate dynamic timeout with exponential backoff
                    timeout = base_timeout * (1.5 ** handshake_retry_count)
                    
                    try:
                        # Send handshake with retry counter for debugging
                        handshake.data["retry"] = handshake_retry_count
                        handshake.data["timestamp"] = time.time()  # Update timestamp
                        await conn.send_message(handshake)
                        
                        # Wait with clear logging
                        logger.info(f"Waiting for handshake response from {node_id} (attempt {handshake_retry_count+1}/{max_handshake_retries}, timeout: {timeout:.1f}s)")
                        
                        # Improved message receiving with clearer error handling
                        try:
                            response = await asyncio.wait_for(conn.receive_message(), timeout=timeout)
                        except asyncio.CancelledError:
                            logger.warning(f"Handshake wait cancelled for node {node_id}")
                            raise
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout ({timeout:.1f}s) waiting for handshake response from {node_id}")
                            handshake_retry_count += 1
                            await asyncio.sleep(0.5)  # Brief pause between retries
                            continue
                        
                        # Process the response with clear validation steps
                        if not response:
                            logger.warning(f"Received empty handshake response from {node_id} (attempt {handshake_retry_count+1}/{max_handshake_retries})")
                            handshake_retry_count += 1
                            await asyncio.sleep(0.5)
                            continue
                            
                        # Step 1: Validate message type
                        if response.message_type != MessageType.HANDSHAKE:
                            logger.warning(f"Received non-handshake message type from {node_id}: {response.message_type}")
                            handshake_retry_count += 1
                            if handshake_retry_count < max_handshake_retries:
                                await asyncio.sleep(0.5)
                                continue
                            else:
                                raise ConnectionError(f"Expected handshake message, got {response.message_type}")
                        
                        # Step 2: Validate message source
                        if response.source_id != node_id:
                            logger.warning(f"Handshake response from unexpected source: {response.source_id} (expected {node_id})")
                            handshake_retry_count += 1
                            if handshake_retry_count < max_handshake_retries:
                                await asyncio.sleep(0.5)
                                continue
                            else:
                                raise ConnectionError(f"Handshake source mismatch: {response.source_id} vs {node_id}")
                        
                        # Step 3: Validate response status
                        if not response.data or "status" not in response.data:
                            logger.warning(f"Missing status field in handshake response from {node_id}")
                            handshake_retry_count += 1
                            if handshake_retry_count < max_handshake_retries:
                                await asyncio.sleep(0.5)
                                continue
                            else:
                                raise ConnectionError("Missing status in handshake response")
                        
                        # Step 4: Check acceptance status
                        if response.data.get("status") != "accepted":
                            reason = response.data.get("reason", "No reason provided")
                            logger.warning(f"Handshake rejected by {node_id}: {reason}")
                            raise ConnectionError(f"Handshake rejected: {reason}")
                        
                        # Success! We have a valid handshake response
                        logger.info(f"Received valid handshake response from {node_id}")
                        break
                        
                    except asyncio.TimeoutError:
                        # This should not happen due to our inner try-except, but we handle it as a safeguard
                        handshake_retry_count += 1
                        logger.warning(f"Outer timeout waiting for handshake from {node_id} (attempt {handshake_retry_count}/{max_handshake_retries})")
                        
                    except ConnectionError as e:
                        logger.error(f"Connection error during handshake with {node_id}: {e}")
                        handshake_retry_count += 1
                        if handshake_retry_count >= max_handshake_retries:
                            raise
                        await asyncio.sleep(1.0)  # Longer delay on connection errors
                        
                    except Exception as e:
                        logger.error(f"Unexpected error during handshake with {node_id}: {e}", exc_info=True)
                        handshake_retry_count += 1
                        if handshake_retry_count >= max_handshake_retries:
                            raise ConnectionError(f"Handshake failed: {e}")
                        await asyncio.sleep(1.0)
                
                # Final validation check
                if not response:
                    raise ConnectionError(f"Failed to get valid handshake response from {node_id} after {max_handshake_retries} attempts")
                
                logger.info(f"Handshake completed with node {node_id}")
                
                # Start message processing task
                self.loop.create_task(self._process_connection(conn))
                
                # Successful connection, return
                return
                
            except (ConnectionError, OSError) as e:
                if node_id in self.connections:
                    await self.connections[node_id].close()
                    del self.connections[node_id]
                
                # If we've hit max retries, raise the error
                retry_count += 1
                if retry_count >= max_retries:
                    raise ConnectionError(f"Failed to connect to node {node_id} at {address} after {max_retries} attempts: {e}")
                
                # Otherwise, wait and retry
                logger.info(f"Connection attempt failed ({e}), retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                # Increase the delay for the next retry (exponential backoff)
                retry_delay *= 1.5
            
    async def _process_connection(self, conn: Connection) -> None:
        """
        Process messages from a connection.
        
        Args:
            conn: Connection to process
        """
        try:
            while conn.connected and self.running:
                message = await conn.receive_message()
                
                if not message:
                    break
                    
                # Check if message is for us
                if message.target_id != self.node_id:
                    logger.warning(f"Received message for {message.target_id} from {conn.node_id}")
                    continue
                    
                # Handle special message types
                if message.message_type == MessageType.DISCONNECT:
                    logger.info(f"Received disconnect from node {conn.node_id}")
                    break
                elif message.message_type == MessageType.PING:
                    # Respond to ping
                    response = Message(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.PONG,
                        source_id=self.node_id,
                        target_id=conn.node_id,
                        data=message.data
                    )
                    await conn.send_message(response)
                    continue
                    
                # Pass to callback
                await self._process_message(message)
                
        except Exception as e:
            logger.error(f"Error processing messages from node {conn.node_id}: {e}")
            
        finally:
            # Clean up connection
            await conn.close()
            
            if conn.node_id in self.connections and self.connections[conn.node_id] is conn:
                del self.connections[conn.node_id]
                
            logger.info(f"Connection closed with node {conn.node_id}")
            
    async def disconnect(self, node_id: str) -> None:
        """
        Disconnect from a node.
        
        Args:
            node_id: ID of the node to disconnect from
        """
        if node_id not in self.connections:
            return
            
        conn = self.connections[node_id]
        
        try:
            # Send disconnect message
            disconnect = Message(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.DISCONNECT,
                source_id=self.node_id,
                target_id=node_id,
                data={"reason": "requested"}
            )
            await conn.send_message(disconnect)
            
        except ConnectionError:
            pass
            
        finally:
            # Close connection
            await conn.close()
            
            if node_id in self.connections:
                del self.connections[node_id]
                
    async def send_message(self, target_id: str, message_type: MessageType, 
                           data: Any) -> None:
        """
        Send a message to another node.
        
        Args:
            target_id: ID of the target node
            message_type: Type of message
            data: Message payload
        """
        if target_id not in self.connections:
            raise ConnectionError(f"Not connected to node {target_id}")
            
        # Create message
        message = Message(
            message_id=str(uuid.uuid4()),
            message_type=message_type,
            source_id=self.node_id,
            target_id=target_id,
            data=data
        )
        
        # Send message
        await self.connections[target_id].send_message(message)
        
    async def wait_for_message(self, source_id: str, message_type: MessageType, 
                               timeout: float = None) -> Message:
        """
        Wait for a specific message.
        
        Args:
            source_id: ID of the source node
            message_type: Type of message to wait for
            timeout: Timeout in seconds
            
        Returns:
            Received message
            
        Raises:
            asyncio.TimeoutError: If the timeout is reached
        """
        queue_key = f"{source_id}:{message_type.value}"
        
        # Create queue if it doesn't exist
        if queue_key not in self.message_queues:
            self.message_queues[queue_key] = asyncio.Queue()
            
        # Wait for message
        try:
            if timeout is not None:
                message = await asyncio.wait_for(
                    self.message_queues[queue_key].get(), timeout)
            else:
                message = await self.message_queues[queue_key].get()
                
            return message
            
        finally:
            # Clean up empty queue
            if queue_key in self.message_queues and self.message_queues[queue_key].empty():
                del self.message_queues[queue_key]
                
    async def _heartbeat_task(self) -> None:
        """Periodic task to send heartbeats to connected nodes."""
        while self.running:
            try:
                await asyncio.sleep(30)  # Heartbeat every 30 seconds
                
                current_time = time.time()
                
                # Check and send pings to idle connections
                for node_id, conn in list(self.connections.items()):
                    if not conn.connected:
                        # Remove closed connection
                        del self.connections[node_id]
                        continue
                        
                    if current_time - conn.last_activity > 25:  # Idle for 25+ seconds
                        try:
                            # Send ping
                            ping = Message(
                                message_id=str(uuid.uuid4()),
                                message_type=MessageType.PING,
                                source_id=self.node_id,
                                target_id=node_id,
                                data={"timestamp": current_time}
                            )
                            await conn.send_message(ping)
                            
                        except ConnectionError:
                            # Connection is dead, remove it
                            if node_id in self.connections:
                                del self.connections[node_id]
                                
            except Exception as e:
                logger.error(f"Error in heartbeat task: {e}")
