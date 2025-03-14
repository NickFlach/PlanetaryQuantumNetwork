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
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "data": self.data,
            "timestamp": self.timestamp
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create a message from a dictionary."""
        return cls(
            message_id=data["message_id"],
            message_type=MessageType(data["message_type"]),
            source_id=data["source_id"],
            target_id=data["target_id"],
            data=data["data"],
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
            
        # Serialize the message
        message_data = json.dumps(message.to_dict()).encode()
        
        # Send the message length first, then the message
        message_length = len(message_data)
        length_bytes = message_length.to_bytes(4, byteorder='big')
        
        try:
            self.writer.write(length_bytes)
            self.writer.write(message_data)
            await self.writer.drain()
            self.last_activity = time.time()
        except (ConnectionError, BrokenPipeError, OSError) as e:
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
            # Read message length
            length_bytes = await self.reader.readexactly(4)
            message_length = int.from_bytes(length_bytes, byteorder='big')
            
            # Read message data
            message_data = await self.reader.readexactly(message_length)
            
            # Parse message
            message_dict = json.loads(message_data.decode())
            message = Message.from_dict(message_dict)
            
            self.last_activity = time.time()
            return message
            
        except asyncio.IncompleteReadError:
            # Connection closed
            self.connected = False
            return None
        except (ConnectionError, OSError):
            self.connected = False
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message from node {self.node_id}: {e}")
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
            # Wait for handshake message
            message = await conn.receive_message()
            
            if not message or message.message_type != MessageType.HANDSHAKE:
                logger.warning(f"Invalid handshake from {peer_addr}")
                await conn.close()
                return
                
            # Extract node ID from handshake
            node_id = message.source_id
            
            if node_id in self.connections:
                # Close existing connection
                old_conn = self.connections[node_id]
                await old_conn.close()
                
            # Update connection with node ID
            conn.node_id = node_id
            self.connections[node_id] = conn
            
            logger.info(f"Handshake completed with node {node_id} at {peer_addr}")
            
            # Send handshake response
            response = Message(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.HANDSHAKE,
                source_id=self.node_id,
                target_id=node_id,
                data={"status": "accepted"}
            )
            await conn.send_message(response)
            
            # Process messages
            while conn.connected and self.running:
                message = await conn.receive_message()
                
                if not message:
                    break
                    
                # Check if message is for us
                if message.target_id != self.node_id:
                    logger.warning(f"Received message for {message.target_id} from {node_id}")
                    continue
                    
                # Handle special message types
                if message.message_type == MessageType.DISCONNECT:
                    logger.info(f"Received disconnect from node {node_id}")
                    break
                elif message.message_type == MessageType.PING:
                    # Respond to ping
                    response = Message(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.PONG,
                        source_id=self.node_id,
                        target_id=node_id,
                        data=message.data
                    )
                    await conn.send_message(response)
                    continue
                    
                # Pass to callback
                await self._process_message(message)
                
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
        
        try:
            # Connect
            reader, writer = await asyncio.open_connection(host, port)
            
            # Create connection
            conn = Connection(node_id, reader, writer)
            self.connections[node_id] = conn
            
            logger.info(f"Connected to node {node_id} at {address}")
            
            # Send handshake
            handshake = Message(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.HANDSHAKE,
                source_id=self.node_id,
                target_id=node_id,
                data={"version": "1.0"}
            )
            await conn.send_message(handshake)
            
            # Wait for handshake response
            response = await conn.receive_message()
            
            if not response or response.message_type != MessageType.HANDSHAKE:
                logger.warning(f"Invalid handshake response from node {node_id}")
                await conn.close()
                del self.connections[node_id]
                raise ConnectionError(f"Invalid handshake response from node {node_id}")
                
            logger.info(f"Handshake completed with node {node_id}")
            
            # Start message processing task
            self.loop.create_task(self._process_connection(conn))
            
        except (ConnectionError, OSError) as e:
            if node_id in self.connections:
                await self.connections[node_id].close()
                del self.connections[node_id]
                
            raise ConnectionError(f"Failed to connect to node {node_id} at {address}: {e}")
            
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
