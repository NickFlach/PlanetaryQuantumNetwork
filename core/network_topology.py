"""
Network Topology Module

This module defines the topology of the distributed OT network,
with a focus on inter-planetary deployment.

Key components:
- Node representation
- Network graph
- Routing algorithms
- Consistency mechanisms for high latency environments
"""

import uuid
import time
import math
import heapq
import logging
from typing import Dict, List, Tuple, Set, Optional, Any, Callable

logger = logging.getLogger(__name__)

class Node:
    """Represents a node in the distributed network."""
    
    def __init__(self, 
                 node_id: str,
                 planet_id: str,
                 coordinates: Optional[Tuple[float, float, float]] = None,
                 capabilities: Optional[Dict[str, Any]] = None):
        """
        Initialize a network node.
        
        Args:
            node_id: Unique identifier for the node
            planet_id: Identifier for the planet where the node is located
            coordinates: 3D coordinates in space (optional)
            capabilities: Dictionary of node capabilities
        """
        self.node_id = node_id
        self.planet_id = planet_id
        self.coordinates = coordinates or (0.0, 0.0, 0.0)
        self.capabilities = capabilities or {}
        self.neighbors: Dict[str, float] = {}  # node_id -> latency
        self.online = True
        self.last_seen = time.time()
        
    def add_neighbor(self, node_id: str, latency: float) -> None:
        """
        Add a neighbor to this node.
        
        Args:
            node_id: ID of the neighbor node
            latency: Latency to the neighbor in milliseconds
        """
        self.neighbors[node_id] = latency
        
    def remove_neighbor(self, node_id: str) -> None:
        """
        Remove a neighbor from this node.
        
        Args:
            node_id: ID of the neighbor node to remove
        """
        if node_id in self.neighbors:
            del self.neighbors[node_id]
            
    def get_neighbors(self) -> Dict[str, float]:
        """
        Get all neighbors of this node.
        
        Returns:
            Dictionary mapping neighbor node IDs to latencies
        """
        return self.neighbors
        
    def to_dict(self) -> Dict:
        """Convert node to dictionary representation."""
        return {
            "node_id": self.node_id,
            "planet_id": self.planet_id,
            "coordinates": self.coordinates,
            "capabilities": self.capabilities,
            "neighbors": self.neighbors,
            "online": self.online,
            "last_seen": self.last_seen
        }
        
    @classmethod
    def from_dict(cls, data: Dict) -> 'Node':
        """Create node from dictionary representation."""
        node = cls(
            node_id=data["node_id"],
            planet_id=data["planet_id"],
            coordinates=data["coordinates"],
            capabilities=data["capabilities"]
        )
        node.neighbors = data["neighbors"]
        node.online = data["online"]
        node.last_seen = data["last_seen"]
        return node


class NetworkTopology:
    """Manages the topology of the distributed network."""
    
    def __init__(self):
        """Initialize the network topology."""
        self.nodes: Dict[str, Node] = {}  # node_id -> Node
        self.planets: Dict[str, Set[str]] = {}  # planet_id -> set of node_ids
        
    def add_node(self, node: Node) -> None:
        """
        Add a node to the network.
        
        Args:
            node: Node to add
        """
        self.nodes[node.node_id] = node
        
        # Add node to planet
        if node.planet_id not in self.planets:
            self.planets[node.planet_id] = set()
        self.planets[node.planet_id].add(node.node_id)
        
    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the network.
        
        Args:
            node_id: ID of the node to remove
        """
        if node_id not in self.nodes:
            return
            
        node = self.nodes[node_id]
        
        # Remove node from planet
        if node.planet_id in self.planets and node_id in self.planets[node.planet_id]:
            self.planets[node.planet_id].remove(node_id)
            
        # Remove node from neighbors' lists
        for other_node in self.nodes.values():
            if node_id in other_node.neighbors:
                other_node.remove_neighbor(node_id)
                
        # Delete node
        del self.nodes[node_id]
        
    def add_link(self, node_id1: str, node_id2: str, latency: float) -> None:
        """
        Add a bidirectional link between two nodes.
        
        Args:
            node_id1: ID of the first node
            node_id2: ID of the second node
            latency: Latency between the nodes in milliseconds
        """
        if node_id1 not in self.nodes or node_id2 not in self.nodes:
            raise ValueError(f"One or both nodes not found: {node_id1}, {node_id2}")
            
        self.nodes[node_id1].add_neighbor(node_id2, latency)
        self.nodes[node_id2].add_neighbor(node_id1, latency)
        
    def remove_link(self, node_id1: str, node_id2: str) -> None:
        """
        Remove a link between two nodes.
        
        Args:
            node_id1: ID of the first node
            node_id2: ID of the second node
        """
        if node_id1 in self.nodes:
            self.nodes[node_id1].remove_neighbor(node_id2)
            
        if node_id2 in self.nodes:
            self.nodes[node_id2].remove_neighbor(node_id1)
            
    def get_planet_nodes(self, planet_id: str) -> List[Node]:
        """
        Get all nodes on a specific planet.
        
        Args:
            planet_id: ID of the planet
            
        Returns:
            List of nodes on the planet
        """
        if planet_id not in self.planets:
            return []
            
        return [self.nodes[node_id] for node_id in self.planets[planet_id] 
                if node_id in self.nodes]
                
    def find_shortest_path(self, source_id: str, target_id: str) -> Tuple[List[str], float]:
        """
        Find the shortest path between two nodes using Dijkstra's algorithm.
        
        Args:
            source_id: ID of the source node
            target_id: ID of the target node
            
        Returns:
            Tuple containing the path (list of node IDs) and total latency
        """
        if source_id not in self.nodes or target_id not in self.nodes:
            raise ValueError(f"Source or target node not found: {source_id}, {target_id}")
            
        # Initialize distances and previous nodes
        distances = {node_id: float('infinity') for node_id in self.nodes}
        previous = {node_id: None for node_id in self.nodes}
        distances[source_id] = 0
        
        # Priority queue for Dijkstra's algorithm
        priority_queue = [(0, source_id)]
        
        while priority_queue:
            current_distance, current_node_id = heapq.heappop(priority_queue)
            
            # If we've reached the target, we're done
            if current_node_id == target_id:
                break
                
            # If we've already found a better path, skip
            if current_distance > distances[current_node_id]:
                continue
                
            # Check all neighbors
            for neighbor_id, latency in self.nodes[current_node_id].get_neighbors().items():
                if neighbor_id not in self.nodes:
                    continue
                    
                distance = current_distance + latency
                
                # If we found a better path, update
                if distance < distances[neighbor_id]:
                    distances[neighbor_id] = distance
                    previous[neighbor_id] = current_node_id
                    heapq.heappush(priority_queue, (distance, neighbor_id))
                    
        # Reconstruct path
        path = []
        current = target_id
        
        while current:
            path.append(current)
            current = previous[current]
            
        path.reverse()
        
        # Check if a path was found
        if path[0] != source_id:
            return [], float('infinity')
            
        return path, distances[target_id]
        
    def find_minimum_spanning_tree(self) -> Dict[str, List[Tuple[str, float]]]:
        """
        Find the minimum spanning tree of the network using Prim's algorithm.
        
        Returns:
            Dictionary mapping node IDs to lists of (neighbor_id, latency) tuples
        """
        if not self.nodes:
            return {}
            
        # Start with any node
        start_node_id = next(iter(self.nodes.keys()))
        
        # Initialize the MST and the set of nodes in the MST
        mst: Dict[str, List[Tuple[str, float]]] = {node_id: [] for node_id in self.nodes}
        in_mst = {node_id: False for node_id in self.nodes}
        
        # Add the start node to the MST
        in_mst[start_node_id] = True
        
        # Priority queue for edges
        edges = []
        
        # Add all edges from the start node
        for neighbor_id, latency in self.nodes[start_node_id].get_neighbors().items():
            if neighbor_id in self.nodes:
                heapq.heappush(edges, (latency, start_node_id, neighbor_id))
                
        # Process edges until all nodes are in the MST
        while edges and not all(in_mst.values()):
            latency, from_node, to_node = heapq.heappop(edges)
            
            # Skip if the target node is already in the MST
            if in_mst[to_node]:
                continue
                
            # Add the edge to the MST
            mst[from_node].append((to_node, latency))
            mst[to_node].append((from_node, latency))
            
            # Mark the target node as in the MST
            in_mst[to_node] = True
            
            # Add all edges from the new node
            for neighbor_id, neighbor_latency in self.nodes[to_node].get_neighbors().items():
                if neighbor_id in self.nodes and not in_mst[neighbor_id]:
                    heapq.heappush(edges, (neighbor_latency, to_node, neighbor_id))
                    
        return mst
        
    def calculate_network_diameter(self) -> float:
        """
        Calculate the diameter of the network (longest shortest path).
        
        Returns:
            Diameter of the network in milliseconds
        """
        max_latency = 0.0
        
        # Calculate the shortest path between every pair of nodes
        for source_id in self.nodes:
            for target_id in self.nodes:
                if source_id == target_id:
                    continue
                    
                _, latency = self.find_shortest_path(source_id, target_id)
                max_latency = max(max_latency, latency)
                
        return max_latency
        
    def get_interplanetary_links(self) -> List[Tuple[str, str, float]]:
        """
        Get all links between nodes on different planets.
        
        Returns:
            List of (node_id1, node_id2, latency) tuples
        """
        interplanetary_links = []
        
        # Check each node
        for node_id, node in self.nodes.items():
            # Check each neighbor
            for neighbor_id, latency in node.get_neighbors().items():
                if neighbor_id not in self.nodes:
                    continue
                    
                neighbor = self.nodes[neighbor_id]
                
                # If the nodes are on different planets, add the link
                if node.planet_id != neighbor.planet_id:
                    # Avoid duplicates by only adding if the current node ID
                    # is lexicographically smaller than the neighbor ID
                    if node_id < neighbor_id:
                        interplanetary_links.append((node_id, neighbor_id, latency))
                        
        return interplanetary_links
        
    def update_node_status(self, node_id: str, online: bool) -> None:
        """
        Update the status of a node.
        
        Args:
            node_id: ID of the node
            online: Whether the node is online
        """
        if node_id in self.nodes:
            self.nodes[node_id].online = online
            self.nodes[node_id].last_seen = time.time()
            
    def get_optimal_routing_table(self, node_id: str) -> Dict[str, str]:
        """
        Calculate the optimal routing table for a node.
        
        Args:
            node_id: ID of the node
            
        Returns:
            Dictionary mapping target node IDs to next hop node IDs
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node not found: {node_id}")
            
        routing_table = {}
        
        # Calculate shortest paths to all other nodes
        for target_id in self.nodes:
            if target_id == node_id:
                continue
                
            path, _ = self.find_shortest_path(node_id, target_id)
            
            if len(path) >= 2:
                # The next hop is the second node in the path
                routing_table[target_id] = path[1]
                
        return routing_table
        
    def calculate_latency(self, node_id1: str, node_id2: str) -> float:
        """
        Calculate the expected latency between two nodes.
        
        Args:
            node_id1: ID of the first node
            node_id2: ID of the second node
            
        Returns:
            Expected latency in milliseconds
        """
        if node_id1 not in self.nodes or node_id2 not in self.nodes:
            raise ValueError(f"One or both nodes not found: {node_id1}, {node_id2}")
            
        # If the nodes are neighbors, use the direct latency
        if node_id2 in self.nodes[node_id1].get_neighbors():
            return self.nodes[node_id1].get_neighbors()[node_id2]
            
        # Otherwise, find the shortest path
        _, latency = self.find_shortest_path(node_id1, node_id2)
        return latency
        
    def get_all_planets(self) -> List[str]:
        """
        Get all planets in the network.
        
        Returns:
            List of planet IDs
        """
        return list(self.planets.keys())
        
    def get_planet_to_planet_latency(self, planet_id1: str, planet_id2: str) -> float:
        """
        Calculate the minimum latency between two planets.
        
        Args:
            planet_id1: ID of the first planet
            planet_id2: ID of the second planet
            
        Returns:
            Minimum latency between the planets in milliseconds
        """
        if planet_id1 not in self.planets or planet_id2 not in self.planets:
            raise ValueError(f"One or both planets not found: {planet_id1}, {planet_id2}")
            
        # If it's the same planet, latency is 0
        if planet_id1 == planet_id2:
            return 0.0
            
        min_latency = float('infinity')
        
        # Check all pairs of nodes between the planets
        for node_id1 in self.planets[planet_id1]:
            for node_id2 in self.planets[planet_id2]:
                if node_id1 not in self.nodes or node_id2 not in self.nodes:
                    continue
                    
                latency = self.calculate_latency(node_id1, node_id2)
                min_latency = min(min_latency, latency)
                
        return min_latency if min_latency != float('infinity') else -1.0
