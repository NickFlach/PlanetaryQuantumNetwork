"""
Simulation Environment Module

This module provides a simulation environment for testing the distributed OT network
with different network conditions and planetary configurations.

Key components:
- Planetary simulation
- Network simulation with latency models
- Node coordination and synchronization
- Metrics collection and analysis
"""

import asyncio
import random
import time
import uuid
import logging
import json
from typing import Dict, List, Tuple, Any, Optional, Set

from core.ot_engine import Operation, InterPlanetaryOTEngine
from network.planetary_node import PlanetaryNode
from network.communication import MessageHandler
from simulation.latency_models import InterPlanetaryLatencyModel, LatencyModelType

logger = logging.getLogger(__name__)

class SimulatedPlanet:
    """Represents a simulated planet in the environment."""
    
    def __init__(self, 
                 planet_id: str,
                 position: Tuple[float, float, float] = None,
                 node_count: int = 3):
        """
        Initialize a simulated planet.
        
        Args:
            planet_id: Identifier for the planet
            position: 3D coordinates of the planet (x, y, z) in astronomical units
            node_count: Number of nodes to create on the planet
        """
        self.planet_id = planet_id
        self.position = position or (random.uniform(-10, 10), random.uniform(-10, 10), random.uniform(-10, 10))
        self.nodes: List[PlanetaryNode] = []
        self.node_count = node_count
        
    async def initialize(self) -> None:
        """Initialize the planet with nodes."""
        logger.info(f"Initializing planet {self.planet_id} with {self.node_count} nodes")
        try:
            # Create nodes
            for i in range(self.node_count):
                node_id = f"{self.planet_id}-node-{i}"
                logger.info(f"Creating node {node_id}")
                
                # Create node config
                config = {
                    "coordinates": self.position,
                    "capabilities": {
                        "storage": random.randint(1, 10),
                        "bandwidth": random.randint(1, 10),
                        "reliability": random.uniform(0.9, 0.99)
                    },
                    "synch_interval": random.randint(30, 120),
                    "discovery_interval": random.randint(150, 300),
                    "known_nodes": []
                }
                
                # Create node
                logger.info(f"Instantiating PlanetaryNode for {node_id}")
                try:
                    node = PlanetaryNode(node_id=node_id, planet_id=self.planet_id, config=config)
                    logger.info(f"Successfully created node {node_id}")
                except Exception as e:
                    logger.error(f"Error creating node {node_id}: {e}", exc_info=True)
                    raise
                
                # Add to list
                self.nodes.append(node)
                logger.info(f"Added node {node_id} to planet {self.planet_id}")
                
            # Connect nodes on the same planet
            logger.info(f"Setting up intra-planet connections on {self.planet_id}")
            for i, node in enumerate(self.nodes):
                node.config["known_nodes"] = []
                
                # Add other nodes as known nodes
                for j, other_node in enumerate(self.nodes):
                    if i != j:
                        # localhost connection for simulation
                        connection = {
                            "node_id": other_node.node_id,
                            "address": f"127.0.0.1:{8000 + j}"
                        }
                        node.config["known_nodes"].append(connection)
                        logger.info(f"Added connection from {node.node_id} to {other_node.node_id} at {connection['address']}")
        except Exception as e:
            logger.error(f"Error initializing planet {self.planet_id}: {e}", exc_info=True)
            raise
                    
    async def start_nodes(self) -> None:
        """Start all nodes on the planet."""
        logger.info(f"Starting {len(self.nodes)} nodes on planet {self.planet_id}")
        try:
            for i, node in enumerate(self.nodes):
                logger.info(f"Starting node {node.node_id} ({i+1}/{len(self.nodes)})...")
                try:
                    node.start()
                    logger.info(f"Successfully started node {node.node_id}")
                except Exception as e:
                    logger.error(f"Error starting node {node.node_id}: {e}", exc_info=True)
                    raise
                
            logger.info(f"Successfully started all {len(self.nodes)} nodes on planet {self.planet_id}")
        except Exception as e:
            logger.error(f"Error starting nodes on planet {self.planet_id}: {e}", exc_info=True)
            raise
        
    async def stop_nodes(self) -> None:
        """Stop all nodes on the planet."""
        for node in self.nodes:
            node.stop()
            
        logger.info(f"Stopped {len(self.nodes)} nodes on planet {self.planet_id}")


class SimulationEnvironment:
    """Simulation environment for the distributed OT network."""
    
    def __init__(self, 
                 planet_count: int = 3,
                 nodes_per_planet: int = 3,
                 latency_model: LatencyModelType = LatencyModelType.REALISTIC):
        """
        Initialize the simulation environment.
        
        Args:
            planet_count: Number of planets to simulate
            nodes_per_planet: Number of nodes per planet
            latency_model: Type of latency model to use
        """
        self.planet_count = planet_count
        self.nodes_per_planet = nodes_per_planet
        self.latency_model_type = latency_model
        self.latency_model = InterPlanetaryLatencyModel.create(latency_model)
        
        # Planets
        self.planets: Dict[str, SimulatedPlanet] = {}
        
        # All nodes
        self.all_nodes: Dict[str, PlanetaryNode] = {}
        
        # Interplanetary connections
        self.interplanetary_connections: List[Tuple[str, str]] = []
        
        # Metrics
        self.metrics = {
            "operations_total": 0,
            "operations_per_planet": {},
            "operations_timestamp": [],
            "convergence_time": [],
            "operation_latency": [],
            "start_time": time.time()
        }
        
    async def initialize(self) -> None:
        """Initialize the simulation environment."""
        logger.info("Starting simulation environment initialization...")
        try:
            # Create planets
            for i in range(self.planet_count):
                planet_id = f"planet-{i}"
                logger.info(f"Creating planet {planet_id}...")
                
                # Create random position
                position = (
                    random.uniform(-10, 10),
                    random.uniform(-10, 10),
                    random.uniform(-10, 10)
                )
                
                # Create planet
                planet = SimulatedPlanet(
                    planet_id=planet_id,
                    position=position,
                    node_count=self.nodes_per_planet
                )
                
                # Initialize planet
                logger.info(f"Initializing planet {planet_id}...")
                await planet.initialize()
                
                # Add to planet dict
                self.planets[planet_id] = planet
                
                # Add to metrics
                self.metrics["operations_per_planet"][planet_id] = 0
                
                # Add nodes to all_nodes dict
                for node in planet.nodes:
                    self.all_nodes[node.node_id] = node
                    logger.info(f"Added node {node.node_id} to simulation")
                    
            # Setup interplanetary connections
            logger.info("Setting up interplanetary connections...")
            await self._setup_interplanetary_connections()
            
            logger.info(f"Initialized simulation with {len(self.planets)} planets and {len(self.all_nodes)} nodes")
        except Exception as e:
            logger.error(f"Error during simulation initialization: {e}", exc_info=True)
            raise
        
    async def _setup_interplanetary_connections(self) -> None:
        """Setup connections between planets."""
        # Create a list of all planets
        planet_ids = list(self.planets.keys())
        
        # For each planet, connect to at least one other planet
        for planet_id in planet_ids:
            # Choose a random other planet
            other_planets = [p for p in planet_ids if p != planet_id]
            
            if not other_planets:
                continue
                
            other_planet_id = random.choice(other_planets)
            
            # Add to connections
            self.interplanetary_connections.append((planet_id, other_planet_id))
            
            # Choose random nodes from each planet to connect
            source_node = random.choice(self.planets[planet_id].nodes)
            target_node = random.choice(self.planets[other_planet_id].nodes)
            
            # Add as known nodes
            source_node.config["known_nodes"].append({
                "node_id": target_node.node_id,
                "address": f"127.0.0.1:{8000 + int(target_node.node_id.split('-')[-1])}"
            })
            
            target_node.config["known_nodes"].append({
                "node_id": source_node.node_id,
                "address": f"127.0.0.1:{8000 + int(source_node.node_id.split('-')[-1])}"
            })
            
        logger.info(f"Setup {len(self.interplanetary_connections)} interplanetary connections")
        
    async def start(self) -> None:
        """Start the simulation."""
        logger.info("Starting simulation nodes...")
        try:
            # Start all planet nodes
            for planet_id, planet in self.planets.items():
                logger.info(f"Starting nodes on planet {planet_id}...")
                await planet.start_nodes()
                
            # Allow time for nodes to connect
            logger.info("Waiting for nodes to connect...")
            await asyncio.sleep(5)
            
            logger.info("Simulation started successfully")
        except Exception as e:
            logger.error(f"Error during simulation start: {e}", exc_info=True)
            raise
        
    async def stop(self) -> None:
        """Stop the simulation."""
        # Stop all planet nodes
        for planet in self.planets.values():
            await planet.stop_nodes()
            
        logger.info("Simulation stopped")
        
    async def inject_operation(self, 
                              planet_id: Optional[str] = None, 
                              operation: Optional[Operation] = None) -> Operation:
        """
        Inject an operation into the simulation.
        
        Args:
            planet_id: ID of the planet to inject the operation into (random if None)
            operation: Operation to inject (random if None)
            
        Returns:
            The injected operation
        """
        # Choose a random planet if not specified
        if planet_id is None:
            planet_id = random.choice(list(self.planets.keys()))
            
        # Choose a random node from the planet
        planet = self.planets[planet_id]
        node = random.choice(planet.nodes)
        
        # Create a random operation if not specified
        if operation is None:
            # Get current document
            document = await node.get_document_state()
            
            # Create a random operation
            if random.random() < 0.7 or not document:  # 70% chance of insert
                position = random.randint(0, len(document)) if document else 0
                content = random.choice(["Hello", "World", " ", "!", "?"]) 
                
                operation = Operation(
                    op_type="insert",
                    position=position,
                    content=content,
                    client_id=node.node_id
                )
            else:  # 30% chance of delete
                position = random.randint(0, len(document) - 1)
                length = min(random.randint(1, 5), len(document) - position)
                
                operation = Operation(
                    op_type="delete",
                    position=position,
                    content=document[position:position+length],
                    client_id=node.node_id
                )
                
        # Inject operation
        start_time = time.time()
        await node.send_operation(operation)
        
        # Record metrics
        self.metrics["operations_total"] += 1
        self.metrics["operations_per_planet"][planet_id] = self.metrics["operations_per_planet"].get(planet_id, 0) + 1
        self.metrics["operations_timestamp"].append((time.time(), operation.to_dict()))
        
        # Monitor convergence
        convergence_time = await self._monitor_convergence(operation, start_time)
        self.metrics["convergence_time"].append(convergence_time)
        self.metrics["operation_latency"].append((operation.operation_id, convergence_time))
        
        logger.info(f"Injected operation {operation.operation_id} on planet {planet_id}")
        
        return operation
        
    async def _monitor_convergence(self, operation: Operation, start_time: float) -> float:
        """
        Monitor the convergence of an operation across all nodes.
        
        Args:
            operation: Operation to monitor
            start_time: Start time of the operation
            
        Returns:
            Time in seconds for the operation to converge
        """
        # Wait for all nodes to converge
        all_converged = False
        max_wait = 300  # Maximum wait time in seconds
        wait_start = time.time()
        
        while not all_converged and time.time() - wait_start < max_wait:
            # Check if all nodes have the operation
            all_converged = True
            
            for node in self.all_nodes.values():
                try:
                    # Check if operation is in history
                    found = False
                    for op in node.ot_engine.history:
                        if op.operation_id == operation.operation_id:
                            found = True
                            break
                            
                    if not found:
                        all_converged = False
                        break
                except Exception:
                    all_converged = False
                    break
                    
            if not all_converged:
                await asyncio.sleep(1)
                
        # Calculate convergence time
        if all_converged:
            convergence_time = time.time() - start_time
        else:
            # If not converged within max_wait, use max_wait as the convergence time
            convergence_time = max_wait
            
        return convergence_time
        
    async def run_simulation(self, 
                            operation_count: int = 100,
                            operations_per_second: float = 1.0) -> Dict[str, Any]:
        """
        Run a simulation with random operations.
        
        Args:
            operation_count: Number of operations to inject
            operations_per_second: Operations per second
            
        Returns:
            Simulation metrics
        """
        # Calculate sleep time between operations
        sleep_time = 1.0 / operations_per_second if operations_per_second > 0 else 0
        
        # Inject operations
        for i in range(operation_count):
            await self.inject_operation()
            
            # Sleep between operations
            if i < operation_count - 1 and sleep_time > 0:
                await asyncio.sleep(sleep_time)
                
        # Calculate final metrics
        self.metrics["duration"] = time.time() - self.metrics["start_time"]
        self.metrics["operations_per_second"] = self.metrics["operations_total"] / self.metrics["duration"]
        
        if self.metrics["convergence_time"]:
            self.metrics["avg_convergence_time"] = sum(self.metrics["convergence_time"]) / len(self.metrics["convergence_time"])
            self.metrics["max_convergence_time"] = max(self.metrics["convergence_time"])
            self.metrics["min_convergence_time"] = min(self.metrics["convergence_time"])
        else:
            self.metrics["avg_convergence_time"] = 0
            self.metrics["max_convergence_time"] = 0
            self.metrics["min_convergence_time"] = 0
            
        return self.metrics
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get the current simulation metrics."""
        # Update duration
        self.metrics["duration"] = time.time() - self.metrics["start_time"]
        
        if self.metrics["operations_total"] > 0:
            self.metrics["operations_per_second"] = self.metrics["operations_total"] / self.metrics["duration"]
            
        if self.metrics["convergence_time"]:
            self.metrics["avg_convergence_time"] = sum(self.metrics["convergence_time"]) / len(self.metrics["convergence_time"])
            self.metrics["max_convergence_time"] = max(self.metrics["convergence_time"])
            self.metrics["min_convergence_time"] = min(self.metrics["convergence_time"])
            
        return self.metrics
        
    async def get_network_state(self) -> Dict[str, Any]:
        """Get the current state of the network."""
        # Collect document state from all nodes
        documents = {}
        
        for node_id, node in self.all_nodes.items():
            try:
                document = await node.get_document_state()
                documents[node_id] = document
            except Exception as e:
                logger.error(f"Error getting document from node {node_id}: {e}")
                documents[node_id] = f"ERROR: {str(e)}"
                
        # Check if all nodes have converged to the same state
        converged = len(set(documents.values())) == 1
        
        # Get network topology from a node
        topology = None
        try:
            node = next(iter(self.all_nodes.values()))
            topology = await node.get_network_topology()
        except Exception as e:
            logger.error(f"Error getting network topology: {e}")
            
        return {
            "documents": documents,
            "converged": converged,
            "topology": topology
        }
        
    async def simulate_network_partition(self, 
                                        planet_id1: str, 
                                        planet_id2: str, 
                                        duration: float = 60) -> None:
        """
        Simulate a network partition between two planets.
        
        Args:
            planet_id1: ID of the first planet
            planet_id2: ID of the second planet
            duration: Duration of the partition in seconds
        """
        if planet_id1 not in self.planets or planet_id2 not in self.planets:
            raise ValueError(f"Invalid planet IDs: {planet_id1}, {planet_id2}")
            
        # Find connections between these planets
        connections = []
        
        for node1 in self.planets[planet_id1].nodes:
            for node2 in self.planets[planet_id2].nodes:
                if node2.node_id in node1.connected_nodes:
                    connections.append((node1, node2))
                    
        if not connections:
            logger.warning(f"No connections found between planets {planet_id1} and {planet_id2}")
            return
            
        logger.info(f"Simulating network partition between planets {planet_id1} and {planet_id2}")
        
        # Disconnect nodes
        for node1, node2 in connections:
            await node1.disconnect_from_node(node2.node_id)
            await node2.disconnect_from_node(node1.node_id)
            
        logger.info(f"Disconnected {len(connections)} connections, partition will last {duration} seconds")
        
        # Wait for the specified duration
        await asyncio.sleep(duration)
        
        # Reconnect nodes
        for node1, node2 in connections:
            try:
                await node1.connect_to_node(
                    node2.node_id, 
                    f"127.0.0.1:{8000 + int(node2.node_id.split('-')[-1])}"
                )
            except Exception as e:
                logger.error(f"Error reconnecting nodes: {e}")
                
        logger.info(f"Reconnected nodes after partition")
