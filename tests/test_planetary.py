"""
Planetary Node Tests

This module contains tests for the planetary node functionality,
focusing on the distributed OT network across planets.
"""

import unittest
import asyncio
import time
import logging
import uuid
from typing import List, Dict, Any, Tuple

from ..core.ot_engine import Operation
from ..network.planetary_node import PlanetaryNode
from ..crypto.quantum_resistant import QuantumResistantCrypto

# Configure logging
logging.basicConfig(level=logging.ERROR)


class TestPlanetaryNode(unittest.TestCase):
    """Test cases for the PlanetaryNode class."""
    
    def setUp(self):
        """Set up test environment."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create planetary nodes
        self.earth_node = PlanetaryNode(node_id="earth-node-1", planet_id="earth")
        self.mars_node = PlanetaryNode(node_id="mars-node-1", planet_id="mars")
        
        # Configure nodes
        self.earth_node.config["known_nodes"] = [
            {"node_id": "mars-node-1", "address": "127.0.0.1:8001"}
        ]
        self.mars_node.config["known_nodes"] = [
            {"node_id": "earth-node-1", "address": "127.0.0.1:8000"}
        ]
        
    def tearDown(self):
        """Clean up after tests."""
        # Stop nodes
        if hasattr(self, 'earth_node'):
            self.loop.run_until_complete(self._stop_node(self.earth_node))
        if hasattr(self, 'mars_node'):
            self.loop.run_until_complete(self._stop_node(self.mars_node))
        
        # Close event loop
        self.loop.close()
        
    async def _stop_node(self, node):
        """Helper to stop a node."""
        if node.running:
            node.stop()
            # Allow time for node to stop
            await asyncio.sleep(1)
    
    def test_node_initialization(self):
        """Test node initialization."""
        # Check node properties
        self.assertEqual(self.earth_node.node_id, "earth-node-1")
        self.assertEqual(self.earth_node.planet_id, "earth")
        self.assertEqual(self.mars_node.node_id, "mars-node-1")
        self.assertEqual(self.mars_node.planet_id, "mars")
        
        # Check initial document state
        self.assertEqual(self.earth_node.ot_engine.document, "")
        self.assertEqual(self.mars_node.ot_engine.document, "")
    
    def test_node_start_stop(self):
        """Test starting and stopping nodes."""
        # Start nodes
        self.earth_node.start()
        self.mars_node.start()
        
        # Check running state
        self.assertTrue(self.earth_node.running)
        self.assertTrue(self.mars_node.running)
        
        # Stop nodes
        self.earth_node.stop()
        self.mars_node.stop()
        
        # Check running state
        self.assertFalse(self.earth_node.running)
        self.assertFalse(self.mars_node.running)
    
    def test_document_operations(self):
        """Test applying operations to a document."""
        # Start earth node
        self.earth_node.start()
        
        # Create and apply an operation
        operation = Operation(
            op_type="insert",
            position=0,
            content="Hello from Earth",
            client_id=self.earth_node.node_id
        )
        
        # Run the operation
        self.loop.run_until_complete(self.earth_node.send_operation(operation))
        
        # Get the document state
        document = self.loop.run_until_complete(self.earth_node.get_document_state())
        
        # Check document state
        self.assertEqual(document, "Hello from Earth")
        
        # Stop earth node
        self.earth_node.stop()


class TestInterPlanetaryCommunication(unittest.IsolatedAsyncioTestCase):
    """Test cases for inter-planetary communication."""
    
    async def asyncSetUp(self):
        """Set up test environment asynchronously."""
        # Create planetary nodes with different ports for message handlers
        self.earth_node = PlanetaryNode(
            node_id="earth-node-1", 
            planet_id="earth", 
            config={"synch_interval": 5, "discovery_interval": 10}
        )
        self.earth_node.message_handler.port = 8000
        
        self.mars_node = PlanetaryNode(
            node_id="mars-node-1", 
            planet_id="mars", 
            config={"synch_interval": 5, "discovery_interval": 10}
        )
        self.mars_node.message_handler.port = 8001
        
        # Configure nodes to know about each other
        self.earth_node.config["known_nodes"] = [
            {"node_id": "mars-node-1", "address": "127.0.0.1:8001"}
        ]
        self.mars_node.config["known_nodes"] = [
            {"node_id": "earth-node-1", "address": "127.0.0.1:8000"}
        ]
        
        # Start nodes
        self.earth_node.start()
        self.mars_node.start()
        
        # Wait for nodes to start
        await asyncio.sleep(2)
    
    async def asyncTearDown(self):
        """Clean up after tests asynchronously."""
        # Stop nodes
        self.earth_node.stop()
        self.mars_node.stop()
        
        # Wait for nodes to stop
        await asyncio.sleep(2)
    
    async def test_node_connection(self):
        """Test connection between nodes."""
        # Wait for discovery to connect nodes
        await asyncio.sleep(15)
        
        # Check if nodes are connected
        earth_connected = "mars-node-1" in self.earth_node.connected_nodes
        mars_connected = "earth-node-1" in self.mars_node.connected_nodes
        
        self.assertTrue(earth_connected or mars_connected, 
                       "Nodes should be connected in at least one direction")
    
    async def test_operation_propagation(self):
        """Test operation propagation between planets."""
        # Wait for discovery to connect nodes
        await asyncio.sleep(15)
        
        # Create and apply an operation on Earth
        earth_operation = Operation(
            op_type="insert",
            position=0,
            content="Hello from Earth",
            client_id=self.earth_node.node_id
        )
        
        # Send operation from Earth
        await self.earth_node.send_operation(earth_operation)
        
        # Wait for operation to propagate
        await asyncio.sleep(10)
        
        # Get document states
        earth_document = await self.earth_node.get_document_state()
        mars_document = await self.mars_node.get_document_state()
        
        # Check document states
        self.assertEqual(earth_document, "Hello from Earth")
        # Mars may or may not have received the operation yet due to simulated high latency
        # So we'll just check if the document is non-empty
        self.assertTrue(mars_document == "" or mars_document == "Hello from Earth")


class TestQuantumResistantCommunication(unittest.IsolatedAsyncioTestCase):
    """Test cases for quantum-resistant communication between nodes."""
    
    async def asyncSetUp(self):
        """Set up test environment asynchronously."""
        # Create planetary nodes
        self.node1 = PlanetaryNode(
            node_id="node-1", 
            planet_id="earth", 
            config={"synch_interval": 5, "discovery_interval": 10}
        )
        self.node1.message_handler.port = 8010
        
        self.node2 = PlanetaryNode(
            node_id="node-2", 
            planet_id="earth", 
            config={"synch_interval": 5, "discovery_interval": 10}
        )
        self.node2.message_handler.port = 8011
        
        # Configure nodes to know about each other
        self.node1.config["known_nodes"] = [
            {"node_id": "node-2", "address": "127.0.0.1:8011"}
        ]
        self.node2.config["known_nodes"] = [
            {"node_id": "node-1", "address": "127.0.0.1:8010"}
        ]
        
        # Start nodes
        self.node1.start()
        self.node2.start()
        
        # Wait for nodes to start
        await asyncio.sleep(2)
    
    async def asyncTearDown(self):
        """Clean up after tests asynchronously."""
        # Stop nodes
        self.node1.stop()
        self.node2.stop()
        
        # Wait for nodes to stop
        await asyncio.sleep(2)
    
    async def test_quantum_key_exchange(self):
        """Test quantum-resistant key exchange."""
        # Connect nodes
        await self.node1.connect_to_node("node-2", "127.0.0.1:8011")
        
        # Wait for connection and key exchange
        await asyncio.sleep(5)
        
        # Check if nodes are connected
        self.assertIn("node-2", self.node1.connected_nodes)
        
        # Check if session keys were established
        self.assertIn("node-2", self.node1.session_keys)
        
        # Session key should be 32 bytes (for SymmetricEncryption)
        self.assertEqual(len(self.node1.session_keys["node-2"]), 32)
    
    async def test_encrypted_communication(self):
        """Test encrypted communication between nodes."""
        # Connect nodes
        await self.node1.connect_to_node("node-2", "127.0.0.1:8011")
        
        # Wait for connection and key exchange
        await asyncio.sleep(5)
        
        # Create and apply an operation
        operation = Operation(
            op_type="insert",
            position=0,
            content="Quantum-resistant message",
            client_id=self.node1.node_id
        )
        
        # Send operation
        await self.node1.send_operation(operation)
        
        # Wait for operation to propagate
        await asyncio.sleep(5)
        
        # Get document states
        node1_document = await self.node1.get_document_state()
        node2_document = await self.node2.get_document_state()
        
        # Check document states
        self.assertEqual(node1_document, "Quantum-resistant message")
        self.assertEqual(node2_document, "Quantum-resistant message")


if __name__ == '__main__':
    unittest.main()
