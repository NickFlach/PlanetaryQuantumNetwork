"""
Planetary Node Module

This module implements the node logic for the distributed OT network
with a focus on inter-planetary deployment.

Key components:
- Node initialization and configuration
- Network discovery and connection management
- Data synchronization across planets
- Communication with other nodes
"""

import os
import time
import uuid
import json
import threading
import logging
import asyncio
import random
from typing import Dict, List, Tuple, Any, Optional, Set, Union, Callable

from core.ot_engine import InterPlanetaryOTEngine, Operation
from core.network_topology import Node, NetworkTopology
from crypto.quantum_resistant import QuantumResistantCrypto, QuantumKEM, SymmetricEncryption
from crypto.zkp import ZKPInterplanetaryAuth
from network.communication import MessageHandler, Message, MessageType

logger = logging.getLogger(__name__)

class PlanetaryNode:
    """
    Represents a node in the inter-planetary OT network.
    """
    
    def __init__(self, 
                 node_id: Optional[str] = None,
                 planet_id: str = "earth",
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize a planetary node.
        
        Args:
            node_id: Unique identifier for the node (optional, auto-generated if not provided)
            planet_id: Identifier for the planet where the node is located
            config: Configuration options for the node
        """
        self.node_id = node_id or str(uuid.uuid4())
        self.planet_id = planet_id
        self.config = config or {}
        
        # Default configuration values
        self.config.setdefault("synch_interval", 60)  # seconds
        self.config.setdefault("discovery_interval", 300)  # seconds
        self.config.setdefault("max_latency", 1000 * 60 * 20)  # 20 minutes in milliseconds
        self.config.setdefault("crypto_refresh_interval", 86400)  # 24 hours in seconds
        
        # Initialize OT engine
        logger.info(f"Initializing OT engine for {self.node_id}")
        self.ot_engine = InterPlanetaryOTEngine(planet_id)
        
        # Initialize network topology
        logger.info(f"Initializing network topology for {self.node_id}")
        self.network = NetworkTopology()
        
        # For simulation purposes, we'll use simplified crypto components
        logger.info(f"Using simplified crypto for simulation with {self.node_id}")
        
        # Session keys for communicating with other nodes
        self.session_keys: Dict[str, bytes] = {}
        
        # Simplified key generation
        self.private_key = os.urandom(32)  # simple 32-byte key
        self.public_key = hashlib.sha256(self.private_key).digest()  # derive public key
        logger.info(f"Generated simplified keys for {self.node_id}")
        
        # Authentication system (simplified)
        self.auth = {"node_id": self.node_id, "auth_key": os.urandom(16)}
        logger.info(f"Using simplified authentication for {self.node_id}")
        
        # Message handler
        self.message_handler = MessageHandler(self.handle_message)
        
        # Connected nodes
        self.connected_nodes: Set[str] = set()
        
        # Document state
        self.document = ""
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Running flag
        self.running = False
        
        # Event loop
        self.loop = asyncio.get_event_loop()
        
        # Add self to network topology
        self._add_self_to_network()
        
    def _add_self_to_network(self) -> None:
        """Add this node to the network topology."""
        node = Node(
            node_id=self.node_id,
            planet_id=self.planet_id,
            coordinates=self.config.get("coordinates"),
            capabilities=self.config.get("capabilities", {})
        )
        self.network.add_node(node)
        
    def start(self) -> None:
        """Start the node."""
        with self.lock:
            if self.running:
                return
                
            self.running = True
            
        # Start message handler
        self.message_handler.start()
        
        # Start background tasks
        self.loop.create_task(self._discovery_task())
        self.loop.create_task(self._synchronization_task())
        self.loop.create_task(self._crypto_refresh_task())
        
        logger.info(f"Node {self.node_id} started on planet {self.planet_id}")
        
    def stop(self) -> None:
        """Stop the node."""
        with self.lock:
            if not self.running:
                return
                
            self.running = False
            
        # Stop message handler
        self.message_handler.stop()
        
        logger.info(f"Node {self.node_id} stopped")
        
    async def connect_to_node(self, target_id: str, address: str) -> bool:
        """
        Connect to another node in the network.
        
        Args:
            target_id: ID of the target node
            address: Network address of the target node
            
        Returns:
            True if connection was successful, False otherwise
        """
        if target_id in self.connected_nodes:
            logger.info(f"Already connected to node {target_id}")
            return True
            
        logger.info(f"Connecting to node {target_id} at {address}")
        
        try:
            # Establish connection
            await self.message_handler.connect(target_id, address)
            
            # Perform key exchange
            success = await self._key_exchange(target_id)
            
            if success:
                with self.lock:
                    self.connected_nodes.add(target_id)
                    
                # Get node information
                await self._request_node_info(target_id)
                
                logger.info(f"Successfully connected to node {target_id}")
                return True
            else:
                logger.error(f"Key exchange failed with node {target_id}")
                await self.message_handler.disconnect(target_id)
                return False
                
        except Exception as e:
            logger.error(f"Failed to connect to node {target_id}: {e}")
            return False
            
    async def disconnect_from_node(self, target_id: str) -> None:
        """
        Disconnect from a node.
        
        Args:
            target_id: ID of the node to disconnect from
        """
        if target_id not in self.connected_nodes:
            return
            
        logger.info(f"Disconnecting from node {target_id}")
        
        await self.message_handler.disconnect(target_id)
        
        with self.lock:
            self.connected_nodes.remove(target_id)
            if target_id in self.session_keys:
                del self.session_keys[target_id]
                
    async def send_operation(self, operation: Operation) -> None:
        """
        Send an operation to all connected nodes.
        
        Args:
            operation: Operation to send
        """
        # Apply operation locally
        with self.lock:
            self.document = self.ot_engine.local_operation(operation)
            
        # Send operation to all connected nodes
        await self._broadcast_operation(operation)
        
    async def _broadcast_operation(self, operation: Operation) -> None:
        """
        Broadcast an operation to all connected nodes.
        
        Args:
            operation: Operation to broadcast
        """
        message = {
            "type": "operation",
            "data": operation.to_dict()
        }
        
        encoded_message = json.dumps(message).encode()
        
        tasks = []
        for node_id in self.connected_nodes:
            if node_id in self.session_keys:
                # Encrypt message with session key
                encrypted_message = SymmetricEncryption.encrypt(
                    self.session_keys[node_id], encoded_message)
                    
                # Send message
                tasks.append(self.message_handler.send_message(
                    target_id=node_id,
                    message_type=MessageType.ENCRYPTED_DATA,
                    data=encrypted_message
                ))
                
        # Wait for all messages to be sent
        if tasks:
            await asyncio.gather(*tasks)
            
    async def _key_exchange(self, target_id: str) -> bool:
        """
        Perform a key exchange with another node.
        
        Args:
            target_id: ID of the target node
            
        Returns:
            True if key exchange was successful, False otherwise
        """
        # Send public key
        await self.message_handler.send_message(
            target_id=target_id,
            message_type=MessageType.KEY_EXCHANGE,
            data={"node_id": self.node_id, "public_key": self.public_key}
        )
        
        # Wait for public key response
        try:
            response = await asyncio.wait_for(
                self.message_handler.wait_for_message(
                    source_id=target_id,
                    message_type=MessageType.KEY_EXCHANGE
                ),
                timeout=30.0
            )
            
            target_public_key = response.data["public_key"]
            
            # Generate symmetric key
            symmetric_key, encapsulation = self.quantum_kem.encapsulate(target_public_key)
            
            # Send encapsulated key
            await self.message_handler.send_message(
                target_id=target_id,
                message_type=MessageType.KEY_ENCAPSULATION,
                data={"encapsulation": encapsulation}
            )
            
            # Wait for response
            response = await asyncio.wait_for(
                self.message_handler.wait_for_message(
                    source_id=target_id,
                    message_type=MessageType.KEY_CONFIRMATION
                ),
                timeout=30.0
            )
            
            # Store session key
            with self.lock:
                self.session_keys[target_id] = symmetric_key
                
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout during key exchange with node {target_id}")
            return False
        except Exception as e:
            logger.error(f"Error during key exchange with node {target_id}: {e}")
            return False
            
    async def _request_node_info(self, target_id: str) -> None:
        """
        Request information about a node.
        
        Args:
            target_id: ID of the target node
        """
        if target_id not in self.session_keys:
            logger.error(f"No session key for node {target_id}")
            return
            
        message = {
            "type": "node_info_request",
            "data": {"requesting_node": self.node_id}
        }
        
        encoded_message = json.dumps(message).encode()
        encrypted_message = SymmetricEncryption.encrypt(
            self.session_keys[target_id], encoded_message)
            
        await self.message_handler.send_message(
            target_id=target_id,
            message_type=MessageType.ENCRYPTED_DATA,
            data=encrypted_message
        )
        
    async def handle_message(self, message: Message) -> None:
        """
        Handle a received message.
        
        Args:
            message: Received message
        """
        source_id = message.source_id
        
        if message.message_type == MessageType.KEY_EXCHANGE:
            await self._handle_key_exchange(source_id, message.data)
            
        elif message.message_type == MessageType.KEY_ENCAPSULATION:
            await self._handle_key_encapsulation(source_id, message.data)
            
        elif message.message_type == MessageType.ENCRYPTED_DATA:
            if source_id in self.session_keys:
                await self._handle_encrypted_data(source_id, message.data)
            else:
                logger.warning(f"Received encrypted data from node {source_id} with no session key")
                
        else:
            logger.warning(f"Received unknown message type: {message.message_type}")
            
    async def _handle_key_exchange(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle a key exchange message.
        
        Args:
            source_id: ID of the message source
            data: Message data
        """
        node_id = data["node_id"]
        public_key = data["public_key"]
        
        if node_id != source_id:
            logger.warning(f"Node ID mismatch in key exchange: {node_id} != {source_id}")
            return
            
        # Store node's public key (temporary)
        with self.lock:
            self._temp_public_keys = getattr(self, "_temp_public_keys", {})
            self._temp_public_keys[source_id] = public_key
            
        # Respond with our public key
        await self.message_handler.send_message(
            target_id=source_id,
            message_type=MessageType.KEY_EXCHANGE,
            data={"node_id": self.node_id, "public_key": self.public_key}
        )
        
    async def _handle_key_encapsulation(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle a key encapsulation message.
        
        Args:
            source_id: ID of the message source
            data: Message data
        """
        with self.lock:
            if not hasattr(self, "_temp_public_keys") or source_id not in self._temp_public_keys:
                logger.warning(f"No public key for node {source_id}")
                return
                
        encapsulation = data["encapsulation"]
        
        # Decapsulate the symmetric key
        symmetric_key = self.quantum_kem.decapsulate(self.private_key, encapsulation)
        
        # Store session key
        with self.lock:
            self.session_keys[source_id] = symmetric_key
            self.connected_nodes.add(source_id)
            
        # Send confirmation
        await self.message_handler.send_message(
            target_id=source_id,
            message_type=MessageType.KEY_CONFIRMATION,
            data={"status": "success"}
        )
        
        logger.info(f"Established secure session with node {source_id}")
        
    async def _handle_encrypted_data(self, source_id: str, encrypted_data: bytes) -> None:
        """
        Handle encrypted data.
        
        Args:
            source_id: ID of the message source
            encrypted_data: Encrypted message data
        """
        # Decrypt the message
        decrypted_data = SymmetricEncryption.decrypt(self.session_keys[source_id], encrypted_data)
        
        if decrypted_data is None:
            logger.warning(f"Failed to decrypt message from node {source_id}")
            return
            
        try:
            message = json.loads(decrypted_data.decode())
            message_type = message["type"]
            message_data = message["data"]
            
            if message_type == "operation":
                await self._handle_operation(source_id, message_data)
                
            elif message_type == "node_info_request":
                await self._handle_node_info_request(source_id, message_data)
                
            elif message_type == "node_info_response":
                await self._handle_node_info_response(source_id, message_data)
                
            elif message_type == "topology_update":
                await self._handle_topology_update(source_id, message_data)
                
            else:
                logger.warning(f"Unknown message type: {message_type}")
                
        except json.JSONDecodeError:
            logger.warning(f"Failed to decode JSON message from node {source_id}")
        except KeyError as e:
            logger.warning(f"Missing key in message from node {source_id}: {e}")
        except Exception as e:
            logger.error(f"Error handling message from node {source_id}: {e}")
            
    async def _handle_operation(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle an operation message.
        
        Args:
            source_id: ID of the message source
            data: Operation data
        """
        try:
            operation = Operation.from_dict(data)
            
            # Process the operation
            with self.lock:
                result = self.ot_engine.remote_operation(operation)
                if result is not None:
                    self.document = result
                    
            # Forward the operation to other connected nodes
            await self._forward_operation(source_id, operation)
            
        except Exception as e:
            logger.error(f"Error handling operation from node {source_id}: {e}")
            
    async def _forward_operation(self, source_id: str, operation: Operation) -> None:
        """
        Forward an operation to other connected nodes.
        
        Args:
            source_id: ID of the node that sent the operation
            operation: Operation to forward
        """
        message = {
            "type": "operation",
            "data": operation.to_dict()
        }
        
        encoded_message = json.dumps(message).encode()
        
        tasks = []
        for node_id in self.connected_nodes:
            if node_id != source_id and node_id in self.session_keys:
                # Encrypt message with session key
                encrypted_message = SymmetricEncryption.encrypt(
                    self.session_keys[node_id], encoded_message)
                    
                # Send message
                tasks.append(self.message_handler.send_message(
                    target_id=node_id,
                    message_type=MessageType.ENCRYPTED_DATA,
                    data=encrypted_message
                ))
                
        # Wait for all messages to be sent
        if tasks:
            await asyncio.gather(*tasks)
            
    async def _handle_node_info_request(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle a node information request.
        
        Args:
            source_id: ID of the message source
            data: Request data
        """
        # Prepare node information
        node_info = {
            "node_id": self.node_id,
            "planet_id": self.planet_id,
            "connected_nodes": list(self.connected_nodes),
            "coordinates": self.config.get("coordinates"),
            "capabilities": self.config.get("capabilities", {})
        }
        
        # Send response
        message = {
            "type": "node_info_response",
            "data": node_info
        }
        
        encoded_message = json.dumps(message).encode()
        encrypted_message = SymmetricEncryption.encrypt(
            self.session_keys[source_id], encoded_message)
            
        await self.message_handler.send_message(
            target_id=source_id,
            message_type=MessageType.ENCRYPTED_DATA,
            data=encrypted_message
        )
        
    async def _handle_node_info_response(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle a node information response.
        
        Args:
            source_id: ID of the message source
            data: Node information
        """
        node_id = data["node_id"]
        planet_id = data["planet_id"]
        coordinates = data.get("coordinates")
        capabilities = data.get("capabilities", {})
        connected_nodes = data.get("connected_nodes", [])
        
        if node_id != source_id:
            logger.warning(f"Node ID mismatch in node info response: {node_id} != {source_id}")
            return
            
        # Update network topology
        node = Node(
            node_id=node_id,
            planet_id=planet_id,
            coordinates=coordinates,
            capabilities=capabilities
        )
        
        with self.lock:
            self.network.add_node(node)
            
            # Add links to connected nodes
            for connected_node_id in connected_nodes:
                if connected_node_id in self.network.nodes:
                    # Estimate latency based on planet
                    if planet_id == self.network.nodes[connected_node_id].planet_id:
                        # Same planet: low latency
                        latency = random.uniform(10, 100)  # 10-100 ms
                    else:
                        # Different planet: high latency
                        latency = random.uniform(1000 * 60 * 8, 1000 * 60 * 20)  # 8-20 minutes
                        
                    self.network.add_link(node_id, connected_node_id, latency)
                    
        logger.info(f"Updated network topology with node {node_id} on planet {planet_id}")
        
    async def _handle_topology_update(self, source_id: str, data: Dict[str, Any]) -> None:
        """
        Handle a topology update message.
        
        Args:
            source_id: ID of the message source
            data: Topology update data
        """
        nodes = data.get("nodes", [])
        links = data.get("links", [])
        
        with self.lock:
            # Add nodes
            for node_data in nodes:
                node = Node.from_dict(node_data)
                self.network.add_node(node)
                
            # Add links
            for link_data in links:
                node_id1 = link_data["node_id1"]
                node_id2 = link_data["node_id2"]
                latency = link_data["latency"]
                
                if node_id1 in self.network.nodes and node_id2 in self.network.nodes:
                    self.network.add_link(node_id1, node_id2, latency)
                    
        logger.info(f"Updated network topology with {len(nodes)} nodes and {len(links)} links")
        
    async def _discovery_task(self) -> None:
        """Periodic task for network discovery."""
        while self.running:
            try:
                await self._discover_nodes()
                
                # Sleep until next discovery
                await asyncio.sleep(self.config["discovery_interval"])
                
            except Exception as e:
                logger.error(f"Error in discovery task: {e}")
                await asyncio.sleep(10)  # Sleep a bit before retrying
                
    async def _discover_nodes(self) -> None:
        """Discover nodes in the network."""
        # In a real implementation, this would use a discovery mechanism
        # For now, we'll assume a list of known nodes
        known_nodes = self.config.get("known_nodes", [])
        
        for node_info in known_nodes:
            node_id = node_info["node_id"]
            address = node_info["address"]
            
            if node_id != self.node_id and node_id not in self.connected_nodes:
                await self.connect_to_node(node_id, address)
                
    async def _synchronization_task(self) -> None:
        """Periodic task for data synchronization."""
        while self.running:
            try:
                await self._synchronize_topology()
                
                # Sleep until next synchronization
                await asyncio.sleep(self.config["synch_interval"])
                
            except Exception as e:
                logger.error(f"Error in synchronization task: {e}")
                await asyncio.sleep(10)  # Sleep a bit before retrying
                
    async def _synchronize_topology(self) -> None:
        """Synchronize network topology with connected nodes."""
        with self.lock:
            # Prepare topology update
            nodes = []
            for node_id, node in self.network.nodes.items():
                nodes.append(node.to_dict())
                
            links = []
            for node_id, node in self.network.nodes.items():
                for neighbor_id, latency in node.get_neighbors().items():
                    if node_id < neighbor_id:  # Avoid duplicates
                        links.append({
                            "node_id1": node_id,
                            "node_id2": neighbor_id,
                            "latency": latency
                        })
                        
        # Send topology update to connected nodes
        message = {
            "type": "topology_update",
            "data": {
                "nodes": nodes,
                "links": links
            }
        }
        
        encoded_message = json.dumps(message).encode()
        
        tasks = []
        for node_id in self.connected_nodes:
            if node_id in self.session_keys:
                # Encrypt message with session key
                encrypted_message = SymmetricEncryption.encrypt(
                    self.session_keys[node_id], encoded_message)
                    
                # Send message
                tasks.append(self.message_handler.send_message(
                    target_id=node_id,
                    message_type=MessageType.ENCRYPTED_DATA,
                    data=encrypted_message
                ))
                
        # Wait for all messages to be sent
        if tasks:
            await asyncio.gather(*tasks)
            
    async def _crypto_refresh_task(self) -> None:
        """Periodic task for refreshing cryptographic keys."""
        while self.running:
            try:
                # Sleep until next refresh
                await asyncio.sleep(self.config["crypto_refresh_interval"])
                
                # Refresh keys
                await self._refresh_keys()
                
            except Exception as e:
                logger.error(f"Error in crypto refresh task: {e}")
                await asyncio.sleep(10)  # Sleep a bit before retrying
                
    async def _refresh_keys(self) -> None:
        """Refresh cryptographic keys."""
        logger.info("Refreshing cryptographic keys")
        
        # Generate new keypair
        new_private_key, new_public_key = self.quantum_kem.generate_keypair()
        
        # Store new keys
        with self.lock:
            self.private_key = new_private_key
            self.public_key = new_public_key
            
            # Clear session keys (will be re-established)
            self.session_keys.clear()
            
        # Reconnect to all nodes
        connected_nodes = list(self.connected_nodes)
        
        # Disconnect from all nodes
        for node_id in connected_nodes:
            await self.disconnect_from_node(node_id)
            
        # Reconnect to known nodes
        known_nodes = self.config.get("known_nodes", [])
        
        for node_info in known_nodes:
            node_id = node_info["node_id"]
            address = node_info["address"]
            
            if node_id != self.node_id:
                await self.connect_to_node(node_id, address)
                
    async def get_document_state(self) -> str:
        """
        Get the current document state.
        
        Returns:
            Current document state
        """
        with self.lock:
            return self.document
            
    async def get_network_topology(self) -> Dict[str, Any]:
        """
        Get the current network topology.
        
        Returns:
            Network topology information
        """
        with self.lock:
            nodes = []
            for node_id, node in self.network.nodes.items():
                nodes.append(node.to_dict())
                
            links = []
            for node_id, node in self.network.nodes.items():
                for neighbor_id, latency in node.get_neighbors().items():
                    if node_id < neighbor_id:  # Avoid duplicates
                        links.append({
                            "node_id1": node_id,
                            "node_id2": neighbor_id,
                            "latency": latency
                        })
                        
            planets = {}
            for planet_id, node_ids in self.network.planets.items():
                planets[planet_id] = list(node_ids)
                
            return {
                "nodes": nodes,
                "links": links,
                "planets": planets
            }
            
    async def get_interplanetary_latency(self) -> Dict[str, Dict[str, float]]:
        """
        Get the latency between planets.
        
        Returns:
            Dictionary mapping planet pairs to latency
        """
        with self.lock:
            planets = self.network.get_all_planets()
            
            latencies = {}
            for planet1 in planets:
                latencies[planet1] = {}
                
                for planet2 in planets:
                    try:
                        latency = self.network.get_planet_to_planet_latency(planet1, planet2)
                        latencies[planet1][planet2] = latency
                    except ValueError:
                        latencies[planet1][planet2] = -1
                        
            return latencies
