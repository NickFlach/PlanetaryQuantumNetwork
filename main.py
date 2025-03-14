"""
Main Application Entry Point

This module serves as the main entry point for the distributed OT network,
providing command-line interface and application initialization.
"""

import os
import sys
import time
import uuid
import json
import logging
import argparse
import asyncio
from typing import Dict, Any, Optional, List

from config import get_config
from core.ot_engine import InterPlanetaryOTEngine
from network.planetary_node import PlanetaryNode
from api.server import APIServer
from api.sin_integration import SINClient, SINIntegration
from simulation.environment import SimulationEnvironment
from simulation.latency_models import LatencyModelType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def run_node(config_file: Optional[str] = None, 
                  planet_id: Optional[str] = None,
                  node_id: Optional[str] = None,
                  api_port: Optional[int] = None,
                  node_port: Optional[int] = None) -> None:
    """
    Run a planetary node.
    
    Args:
        config_file: Path to configuration file
        planet_id: Planet identifier
        node_id: Node identifier
        api_port: API server port
        node_port: Node communication port
    """
    # Load configuration
    config = get_config(config_file)
    
    # Override configuration with command-line arguments
    if planet_id:
        config.config_data["node"]["planet_id"] = planet_id
    if node_id:
        config.config_data["node"]["node_id"] = node_id
    if api_port:
        config.config_data["network"]["api_port"] = api_port
    if node_port:
        config.config_data["network"]["node_port"] = node_port
    
    # Get configuration values
    node_config = config.get_node_config()
    network_config = config.get_network_config()
    security_config = config.get_security_config()
    sin_config = config.get_sin_config()
    
    # Create planetary node
    node_id = node_config.get("node_id") or f"{node_config['planet_id']}-{str(uuid.uuid4())[:8]}"
    node = PlanetaryNode(
        node_id=node_id,
        planet_id=node_config["planet_id"],
        config={
            "coordinates": node_config.get("coordinates"),
            "capabilities": node_config.get("capabilities"),
            "synch_interval": network_config.get("sync_interval", 60),
            "discovery_interval": network_config.get("discovery_interval", 300),
            "crypto_refresh_interval": network_config.get("crypto_refresh_interval", 86400),
            "known_nodes": node_config.get("known_nodes", [])
        }
    )
    
    # Set node port
    node.message_handler.port = network_config.get("node_port", 8000)
    
    # Create SIN integration
    sin_client = SINClient(
        api_endpoint=sin_config.get("api_endpoint"),
        api_key=sin_config.get("api_key"),
        verify_ssl=sin_config.get("verify_ssl", True)
    )
    sin_integration = SINIntegration(
        ot_engine=node.ot_engine,
        sin_client=sin_client,
        sync_interval=sin_config.get("sync_interval", 30)
    )
    
    # Create API server
    api_server = APIServer(
        node=node,
        host=network_config.get("bind_address", "0.0.0.0"),
        port=network_config.get("api_port", 5000),
        cors_origins=security_config.get("cors_origins", ["*"]),
        allow_anonymous=security_config.get("allow_anonymous", False)
    )
    
    try:
        # Start node
        logger.info(f"Starting node {node_id} on planet {node_config['planet_id']}")
        node.start()
        
        # Start SIN integration
        await sin_integration.start()
        
        # Start API server
        await api_server.start()
        
        logger.info(f"Node {node_id} started successfully")
        logger.info(f"API server running at http://{network_config.get('bind_address', '0.0.0.0')}:{network_config.get('api_port', 5000)}")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
    finally:
        # Stop components
        await api_server.stop()
        await sin_integration.stop()
        node.stop()
        
        logger.info("Node shutdown complete")

async def run_simulation(config_file: Optional[str] = None, 
                        planet_count: Optional[int] = None,
                        nodes_per_planet: Optional[int] = None,
                        operation_rate: Optional[float] = None,
                        duration: Optional[int] = None,
                        api_port: Optional[int] = 5000,
                        start_node_port: Optional[int] = 8000) -> None:
    """
    Run a simulation of the distributed OT network.
    
    Args:
        config_file: Path to configuration file
        planet_count: Number of planets in the simulation
        nodes_per_planet: Number of nodes per planet
        operation_rate: Operations per second
        duration: Simulation duration in seconds
    """
    # Load configuration
    config = get_config(config_file)
    simulation_config = config.get_simulation_config()
    
    # Override configuration with command-line arguments
    if planet_count:
        simulation_config["planet_count"] = planet_count
    if nodes_per_planet:
        simulation_config["nodes_per_planet"] = nodes_per_planet
    if operation_rate:
        simulation_config["operation_rate"] = operation_rate
    if duration:
        simulation_config["simulation_duration"] = duration
    
    # Get configuration values
    planet_count = simulation_config.get("planet_count", 3)
    nodes_per_planet = simulation_config.get("nodes_per_planet", 3)
    latency_model_str = simulation_config.get("latency_model", "realistic")
    operation_rate = simulation_config.get("operation_rate", 1.0)
    duration = simulation_config.get("simulation_duration", 3600)
    
    # Parse latency model
    latency_model = LatencyModelType.REALISTIC
    if latency_model_str == "fixed":
        latency_model = LatencyModelType.FIXED
    elif latency_model_str == "random":
        latency_model = LatencyModelType.RANDOM
    
    logger.info(f"Starting simulation with {planet_count} planets, {nodes_per_planet} nodes per planet")
    logger.info(f"Latency model: {latency_model.value}, Operation rate: {operation_rate} ops/s, Duration: {duration}s")
    
    # Create simulation environment with dynamic port allocation
    simulation = SimulationEnvironment(
        planet_count=planet_count,
        nodes_per_planet=nodes_per_planet,
        latency_model=latency_model,
        start_node_port=start_node_port
    )
    
    try:
        # Initialize and start simulation
        await simulation.initialize()
        await simulation.start()
        
        # Set up API server for the first node on the first planet
        first_planet = next(iter(simulation.planets.values()))
        first_node = first_planet.nodes[0]
        
        api_server = APIServer(
            node=first_node,
            host="0.0.0.0",
            port=api_port,
            cors_origins=["*"],
            allow_anonymous=True
        )
        
        # Start API server
        await api_server.start()
        
        logger.info(f"API server running at http://0.0.0.0:{api_port}")
        logger.info("Running simulation...")
        
        # Run simulation
        operation_count = int(operation_rate * duration)
        metrics = await simulation.run_simulation(
            operation_count=operation_count,
            operations_per_second=operation_rate
        )
        
        # Print simulation results
        logger.info("Simulation completed")
        logger.info(f"Total operations: {metrics['operations_total']}")
        logger.info(f"Operations per second: {metrics['operations_per_second']:.2f}")
        logger.info(f"Average convergence time: {metrics['avg_convergence_time']:.2f}s")
        logger.info(f"Max convergence time: {metrics['max_convergence_time']:.2f}s")
        logger.info(f"Min convergence time: {metrics['min_convergence_time']:.2f}s")
        
        # Keep API server running
        logger.info("API server still running. Press Ctrl+C to exit.")
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
    finally:
        # Stop simulation
        await simulation.stop()
        
        logger.info("Simulation shutdown complete")

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Distributed OT Network")
    
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--planet", type=str, help="Planet ID")
    parser.add_argument("--node-id", type=str, help="Node ID")
    parser.add_argument("--port", type=int, help="API server port")
    parser.add_argument("--node-port", type=int, help="Node communication port")
    
    # Simulation options
    parser.add_argument("--simulation", action="store_true", help="Run in simulation mode")
    parser.add_argument("--planet-count", type=int, help="Number of planets in simulation")
    parser.add_argument("--nodes-per-planet", type=int, help="Number of nodes per planet")
    parser.add_argument("--operation-rate", type=float, help="Operations per second in simulation")
    parser.add_argument("--duration", type=int, help="Simulation duration in seconds")
    
    return parser.parse_args()

async def main():
    """Main entry point."""
    args = parse_arguments()
    
    try:
        if args.simulation:
            # Run simulation with port parameter to avoid conflicts
            # Use args.port if provided, otherwise default to 5000
            api_port = args.port if args.port else 5000
            # Use args.node_port if provided, otherwise default to 8000
            node_port = args.node_port if args.node_port else 8000
            
            await run_simulation(
                config_file=args.config,
                planet_count=args.planet_count,
                nodes_per_planet=args.nodes_per_planet,
                operation_rate=args.operation_rate,
                duration=args.duration,
                api_port=api_port,
                start_node_port=node_port
            )
        else:
            # Run node
            await run_node(
                config_file=args.config,
                planet_id=args.planet,
                node_id=args.node_id,
                api_port=args.port,
                node_port=args.node_port
            )
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
