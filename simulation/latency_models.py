"""
Latency Models Module

This module implements various latency models for simulating inter-planetary
communication in the distributed OT network.

Key components:
- Abstract latency model interface
- Realistic latency model based on astronomical distances
- Simplified latency model for testing
- Random latency model for chaos testing
"""

import math
import random
import enum
from typing import Dict, Tuple, List, Any, Optional

class LatencyModelType(enum.Enum):
    """Types of latency models."""
    FIXED = "fixed"  # Fixed latency
    RANDOM = "random"  # Random latency
    REALISTIC = "realistic"  # Realistic latency based on distance


class LatencyModel:
    """Abstract base class for latency models."""
    
    def get_latency(self, source: str, target: str, context: Optional[Dict[str, Any]] = None) -> float:
        """
        Get the latency between two points.
        
        Args:
            source: Source identifier
            target: Target identifier
            context: Additional context information
            
        Returns:
            Latency in milliseconds
        """
        raise NotImplementedError("Subclasses must implement get_latency")


class FixedLatencyModel(LatencyModel):
    """Fixed latency model with predefined values."""
    
    def __init__(self, default_latency: float = 100.0, latency_map: Optional[Dict[Tuple[str, str], float]] = None):
        """
        Initialize the fixed latency model.
        
        Args:
            default_latency: Default latency in milliseconds
            latency_map: Map of (source, target) tuples to latency values
        """
        self.default_latency = default_latency
        self.latency_map = latency_map or {}
        
    def get_latency(self, source: str, target: str, context: Optional[Dict[str, Any]] = None) -> float:
        """
        Get the latency between two points.
        
        Args:
            source: Source identifier
            target: Target identifier
            context: Additional context information
            
        Returns:
            Latency in milliseconds
        """
        # Check if there's a specific latency value for this pair
        if (source, target) in self.latency_map:
            return self.latency_map[(source, target)]
            
        # Check if there's a specific latency value for the reverse pair
        if (target, source) in self.latency_map:
            return self.latency_map[(target, source)]
            
        # Otherwise, return the default latency
        return self.default_latency


class RandomLatencyModel(LatencyModel):
    """Random latency model for chaos testing."""
    
    def __init__(self, min_latency: float = 10.0, max_latency: float = 1000.0):
        """
        Initialize the random latency model.
        
        Args:
            min_latency: Minimum latency in milliseconds
            max_latency: Maximum latency in milliseconds
        """
        self.min_latency = min_latency
        self.max_latency = max_latency
        
    def get_latency(self, source: str, target: str, context: Optional[Dict[str, Any]] = None) -> float:
        """
        Get the latency between two points.
        
        Args:
            source: Source identifier
            target: Target identifier
            context: Additional context information
            
        Returns:
            Latency in milliseconds
        """
        # Generate a random latency value
        return random.uniform(self.min_latency, self.max_latency)


class RealisticLatencyModel(LatencyModel):
    """Realistic latency model based on astronomical distances."""
    
    def __init__(self):
        """Initialize the realistic latency model."""
        # Speed of light in vacuum (in km/s)
        self.c = 299792.458
        
        # Astronomical units to kilometers
        self.au_to_km = 149597870.7
        
        # Planet positions (x, y, z) in astronomical units
        self.planet_positions = {}
        
        # Cache of computed latencies
        self.latency_cache = {}
        
    def set_planet_position(self, planet_id: str, position: Tuple[float, float, float]) -> None:
        """
        Set the position of a planet.
        
        Args:
            planet_id: Planet identifier
            position: Position (x, y, z) in astronomical units
        """
        self.planet_positions[planet_id] = position
        
        # Clear cached latencies involving this planet
        keys_to_remove = []
        for key in self.latency_cache:
            source, target = key
            if source == planet_id or target == planet_id:
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            del self.latency_cache[key]
            
    def get_latency(self, source: str, target: str, context: Optional[Dict[str, Any]] = None) -> float:
        """
        Get the latency between two points.
        
        Args:
            source: Source identifier (planet-node-X format)
            target: Target identifier (planet-node-X format)
            context: Additional context information
            
        Returns:
            Latency in milliseconds
        """
        # Extract planet IDs from node IDs
        source_planet = source.split('-')[0] if '-' in source else source
        target_planet = target.split('-')[0] if '-' in target else target
        
        # If the planets are the same, return a low latency
        if source_planet == target_planet:
            return random.uniform(10, 100)  # 10-100 ms within the same planet
            
        # Check cache
        cache_key = (source_planet, target_planet)
        if cache_key in self.latency_cache:
            return self.latency_cache[cache_key]
            
        # If planet positions are not available, return a default interplanetary latency
        if source_planet not in self.planet_positions or target_planet not in self.planet_positions:
            # Default to average Earth-Mars latency (about 12 minutes one-way)
            latency = 12 * 60 * 1000  # 12 minutes in milliseconds
            self.latency_cache[cache_key] = latency
            return latency
            
        # Calculate distance between planets
        pos1 = self.planet_positions[source_planet]
        pos2 = self.planet_positions[target_planet]
        
        distance_au = math.sqrt(
            (pos2[0] - pos1[0]) ** 2 +
            (pos2[1] - pos1[1]) ** 2 +
            (pos2[2] - pos1[2]) ** 2
        )
        
        # Convert to kilometers
        distance_km = distance_au * self.au_to_km
        
        # Calculate time for light to travel this distance (in seconds)
        time_s = distance_km / self.c
        
        # Convert to milliseconds
        latency = time_s * 1000
        
        # Store in cache
        self.latency_cache[cache_key] = latency
        
        return latency


class InterPlanetaryLatencyModel:
    """Factory class for creating latency models."""
    
    @staticmethod
    def create(model_type: LatencyModelType, **kwargs) -> LatencyModel:
        """
        Create a latency model.
        
        Args:
            model_type: Type of latency model to create
            **kwargs: Additional arguments for the latency model
            
        Returns:
            Latency model instance
        """
        if model_type == LatencyModelType.FIXED:
            return FixedLatencyModel(**kwargs)
        elif model_type == LatencyModelType.RANDOM:
            return RandomLatencyModel(**kwargs)
        elif model_type == LatencyModelType.REALISTIC:
            return RealisticLatencyModel()
        else:
            raise ValueError(f"Unknown latency model type: {model_type}")


class PlanetaryPositionCalculator:
    """Calculator for planet positions at a specific time."""
    
    def __init__(self, epoch: float = None):
        """
        Initialize the planetary position calculator.
        
        Args:
            epoch: Epoch time (seconds since J2000)
        """
        self.epoch = epoch or time.time() - 946727935.816  # seconds since J2000
        
        # Orbital elements for planets (a, e, i, L, long. peri., long. asc. node)
        # Values from NASA JPL planetary ephemerides
        # Units: a in AU, angles in degrees
        self.orbital_elements = {
            "mercury": (0.38709927, 0.20563593, 7.00497902, 252.25032350, 77.45779628, 48.33076593),
            "venus": (0.72333566, 0.00677672, 3.39467605, 181.97909950, 131.60246718, 76.67984255),
            "earth": (1.00000261, 0.01671123, -0.00001531, 100.46457166, 102.93768193, 0.0),
            "mars": (1.52371034, 0.09339410, 1.84969142, -4.55343205, -23.94362959, 49.55953891),
            "jupiter": (5.20288700, 0.04838624, 1.30439695, 34.39644051, 14.72847983, 100.47390909),
            "saturn": (9.53667594, 0.05386179, 2.48599187, 49.95424423, 92.59887831, 113.66242448),
            "uranus": (19.18916464, 0.04725744, 0.77263783, 313.23810451, 170.95427630, 74.01692503),
            "neptune": (30.06992276, 0.00859048, 1.77004347, -55.12002969, 44.96476227, 131.78422574)
        }
        
    def get_planet_position(self, planet: str) -> Tuple[float, float, float]:
        """
        Get the position of a planet.
        
        Args:
            planet: Planet name (lowercase)
            
        Returns:
            Position (x, y, z) in astronomical units
        """
        if planet not in self.orbital_elements:
            raise ValueError(f"Unknown planet: {planet}")
            
        # Extract orbital elements
        a, e, i, L, long_peri, long_node = self.orbital_elements[planet]
        
        # Convert angles to radians
        i_rad = math.radians(i)
        long_peri_rad = math.radians(long_peri)
        long_node_rad = math.radians(long_node)
        
        # Calculate mean anomaly
        M = math.radians(L - long_peri)
        
        # Solve Kepler's equation to get eccentric anomaly
        E = M
        for _ in range(10):  # iterative solution
            E = M + e * math.sin(E)
            
        # Calculate true anomaly
        v = 2 * math.atan2(math.sqrt(1 + e) * math.sin(E / 2), math.sqrt(1 - e) * math.cos(E / 2))
        
        # Calculate distance from sun
        r = a * (1 - e * math.cos(E))
        
        # Calculate position in orbital plane
        x_orb = r * math.cos(v)
        y_orb = r * math.sin(v)
        
        # Calculate position in 3D space
        x = (x_orb * (math.cos(long_peri_rad) * math.cos(long_node_rad) - 
                     math.sin(long_peri_rad) * math.sin(long_node_rad) * math.cos(i_rad)) -
             y_orb * (math.sin(long_peri_rad) * math.cos(long_node_rad) + 
                     math.cos(long_peri_rad) * math.sin(long_node_rad) * math.cos(i_rad)))
                     
        y = (x_orb * (math.cos(long_peri_rad) * math.sin(long_node_rad) + 
                     math.sin(long_peri_rad) * math.cos(long_node_rad) * math.cos(i_rad)) +
             y_orb * (math.cos(long_peri_rad) * math.cos(long_node_rad) * math.cos(i_rad) - 
                     math.sin(long_peri_rad) * math.sin(long_node_rad)))
                     
        z = (x_orb * math.sin(long_peri_rad) * math.sin(i_rad) + 
             y_orb * math.cos(long_peri_rad) * math.sin(i_rad))
            
        return (x, y, z)


# Add time module for the PlanetaryPositionCalculator
import time
