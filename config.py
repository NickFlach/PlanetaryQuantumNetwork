"""
Configuration Module

This module provides configuration management for the distributed OT network,
with support for environment-specific settings and defaults.

Key components:
- Configuration loading and validation
- Environment variable integration
- Planet-specific configuration
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for the OT network."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to configuration file (optional)
        """
        self.config_data = {}
        self._load_defaults()
        
        if config_file:
            self._load_from_file(config_file)
            
        self._load_from_env()
        
    def _load_defaults(self) -> None:
        """Load default configuration values."""
        self.config_data = {
            # Network configuration
            "network": {
                "bind_address": "0.0.0.0",
                "api_port": 5000,  # For external access
                "node_port": 8000,  # For inter-node communication
                "max_connections": 100,
                "connection_timeout": 30,  # seconds
                "heartbeat_interval": 30,  # seconds
                "discovery_interval": 300,  # seconds
                "sync_interval": 60,  # seconds
                "crypto_refresh_interval": 86400,  # 24 hours
            },
            
            # Node configuration
            "node": {
                "planet_id": "earth",  # Default planet
                "node_id": None,  # Auto-generated if None
                "coordinates": [0, 0, 0],  # Default coordinates
                "capabilities": {
                    "storage": 10,  # Storage capacity (arbitrary units)
                    "bandwidth": 10,  # Bandwidth capacity (arbitrary units)
                    "reliability": 0.99  # Reliability factor (0-1)
                },
                "known_nodes": []  # List of known nodes to connect to
            },
            
            # Security configuration
            "security": {
                "allow_anonymous": False,  # Allow anonymous access
                "api_tokens": [],  # List of valid API tokens
                "cors_origins": ["*"],  # CORS allowed origins
                "cipher_suite": "post_quantum",  # Encryption suite to use
                "signature_scheme": "lamport"  # Signature scheme to use
            },
            
            # Simulation configuration
            "simulation": {
                "enabled": False,  # Enable simulation mode
                "planet_count": 3,  # Number of planets in simulation
                "nodes_per_planet": 3,  # Number of nodes per planet
                "latency_model": "realistic",  # Latency model type
                "operation_rate": 1.0,  # Operations per second
                "simulation_duration": 3600  # Simulation duration in seconds
            },
            
            # SIN integration configuration
            "sin": {
                "api_endpoint": os.getenv("SIN_API_ENDPOINT", "https://api.sin.example.com"),
                "api_key": os.getenv("SIN_API_KEY", ""),
                "verify_ssl": True,
                "sync_interval": 30  # Synchronization interval in seconds
            },
            
            # Planets configuration
            "planets": {
                "earth": {
                    "latency": {
                        "mars": 1200000,  # 20 minutes in milliseconds
                        "venus": 600000,  # 10 minutes in milliseconds
                        "moon": 2600  # 2.6 seconds in milliseconds
                    }
                },
                "mars": {
                    "latency": {
                        "earth": 1200000,  # 20 minutes in milliseconds
                        "venus": 1800000,  # 30 minutes in milliseconds
                        "moon": 1202600  # Earth-Moon + Earth-Mars
                    }
                }
            }
        }
        
    def _load_from_file(self, config_file: str) -> None:
        """
        Load configuration from a file.
        
        Args:
            config_file: Path to configuration file
        """
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                
            # Merge with existing config
            self._merge_config(file_config)
            logger.info(f"Loaded configuration from {config_file}")
            
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to load configuration from {config_file}: {e}")
            
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        # Network configuration
        self._update_from_env("OT_BIND_ADDRESS", ["network", "bind_address"])
        self._update_from_env("OT_API_PORT", ["network", "api_port"], int)
        self._update_from_env("OT_NODE_PORT", ["network", "node_port"], int)
        self._update_from_env("OT_MAX_CONNECTIONS", ["network", "max_connections"], int)
        self._update_from_env("OT_SYNC_INTERVAL", ["network", "sync_interval"], int)
        
        # Node configuration
        self._update_from_env("OT_PLANET_ID", ["node", "planet_id"])
        self._update_from_env("OT_NODE_ID", ["node", "node_id"])
        
        # Security configuration
        self._update_from_env("OT_ALLOW_ANONYMOUS", ["security", "allow_anonymous"], bool)
        self._update_from_env("OT_CIPHER_SUITE", ["security", "cipher_suite"])
        
        # Simulation configuration
        self._update_from_env("OT_SIMULATION_ENABLED", ["simulation", "enabled"], bool)
        self._update_from_env("OT_PLANET_COUNT", ["simulation", "planet_count"], int)
        self._update_from_env("OT_NODES_PER_PLANET", ["simulation", "nodes_per_planet"], int)
        
        # SIN integration configuration
        self._update_from_env("SIN_API_ENDPOINT", ["sin", "api_endpoint"])
        self._update_from_env("SIN_API_KEY", ["sin", "api_key"])
        
        logger.info("Loaded configuration from environment variables")
        
    def _update_from_env(self, env_var: str, config_path: List[str], value_type: Any = str) -> None:
        """
        Update configuration from an environment variable.
        
        Args:
            env_var: Environment variable name
            config_path: Path to configuration value
            value_type: Type to convert the value to
        """
        value = os.getenv(env_var)
        if value is not None:
            # Convert value to the specified type
            if value_type == bool:
                value = value.lower() in ('true', 'yes', '1', 'y')
            elif value_type == int:
                value = int(value)
            elif value_type == float:
                value = float(value)
                
            # Navigate to the correct position in the config
            config = self.config_data
            for i, key in enumerate(config_path):
                if i == len(config_path) - 1:
                    config[key] = value
                else:
                    config = config[key]
        
    def _merge_config(self, new_config: Dict[str, Any]) -> None:
        """
        Merge a new configuration with the existing one.
        
        Args:
            new_config: New configuration to merge
        """
        def merge_dict(target, source):
            for key, value in source.items():
                if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                    merge_dict(target[key], value)
                else:
                    target[key] = value
                    
        merge_dict(self.config_data, new_config)
        
    def get(self, path: List[str], default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            path: Path to configuration value
            default: Default value if not found
            
        Returns:
            Configuration value
        """
        config = self.config_data
        try:
            for key in path:
                config = config[key]
            return config
        except (KeyError, TypeError):
            return default
            
    def get_node_config(self) -> Dict[str, Any]:
        """
        Get node configuration.
        
        Returns:
            Node configuration dictionary
        """
        return self.config_data.get("node", {})
        
    def get_network_config(self) -> Dict[str, Any]:
        """
        Get network configuration.
        
        Returns:
            Network configuration dictionary
        """
        return self.config_data.get("network", {})
        
    def get_security_config(self) -> Dict[str, Any]:
        """
        Get security configuration.
        
        Returns:
            Security configuration dictionary
        """
        return self.config_data.get("security", {})
        
    def get_simulation_config(self) -> Dict[str, Any]:
        """
        Get simulation configuration.
        
        Returns:
            Simulation configuration dictionary
        """
        return self.config_data.get("simulation", {})
        
    def get_sin_config(self) -> Dict[str, Any]:
        """
        Get SIN integration configuration.
        
        Returns:
            SIN configuration dictionary
        """
        return self.config_data.get("sin", {})
        
    def get_planet_config(self, planet_id: str) -> Dict[str, Any]:
        """
        Get configuration for a specific planet.
        
        Args:
            planet_id: Planet identifier
            
        Returns:
            Planet configuration dictionary
        """
        planets = self.config_data.get("planets", {})
        return planets.get(planet_id, {})
        
    def get_interplanetary_latency(self, source_planet: str, target_planet: str) -> int:
        """
        Get the latency between two planets.
        
        Args:
            source_planet: Source planet identifier
            target_planet: Target planet identifier
            
        Returns:
            Latency in milliseconds
        """
        if source_planet == target_planet:
            return 0
            
        source_config = self.get_planet_config(source_planet)
        latency_config = source_config.get("latency", {})
        
        return latency_config.get(target_planet, 1200000)  # Default to 20 minutes
        
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config_data
        
    def save_to_file(self, filename: str) -> None:
        """
        Save configuration to a file.
        
        Args:
            filename: Path to save the configuration to
        """
        try:
            with open(filename, 'w') as f:
                json.dump(self.config_data, f, indent=2)
                
            logger.info(f"Saved configuration to {filename}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration to {filename}: {e}")


# Singleton configuration instance
config_instance = None

def get_config(config_file: Optional[str] = None) -> Config:
    """
    Get the configuration instance.
    
    Args:
        config_file: Path to configuration file (optional)
        
    Returns:
        Configuration instance
    """
    global config_instance
    if config_instance is None:
        config_instance = Config(config_file)
    return config_instance
