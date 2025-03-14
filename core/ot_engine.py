"""
Operational Transform Engine

This module implements the core Operational Transform algorithms for
collaborative editing in a distributed environment with high latency.

Key components:
- Operation representation
- Transformation functions
- History management
- Conflict resolution mechanisms
"""

import uuid
import time
import json
import logging
from typing import Dict, List, Tuple, Any, Optional

logger = logging.getLogger(__name__)

class Operation:
    """Basic operation class for OT system."""
    
    def __init__(self, 
                 op_type: str,
                 position: int, 
                 content: Any = None,
                 client_id: str = None,
                 timestamp: float = None,
                 operation_id: str = None,
                 planet_id: str = None):
        """
        Initialize an operation.
        
        Args:
            op_type: Type of operation ('insert', 'delete', etc.)
            position: Position in the document
            content: Content to insert (if insert operation)
            client_id: ID of the client that generated the operation
            timestamp: Time when operation was created
            operation_id: Unique ID for the operation
            planet_id: ID of the planet where the operation originated
        """
        self.op_type = op_type
        self.position = position
        self.content = content
        self.client_id = client_id or str(uuid.uuid4())
        self.timestamp = timestamp or time.time()
        self.operation_id = operation_id or str(uuid.uuid4())
        self.planet_id = planet_id
        self.version_vector = {}
        
    def to_dict(self) -> Dict:
        """Convert operation to dictionary."""
        return {
            "op_type": self.op_type,
            "position": self.position,
            "content": self.content,
            "client_id": self.client_id,
            "timestamp": self.timestamp,
            "operation_id": self.operation_id,
            "version_vector": self.version_vector
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Operation':
        """Create operation from dictionary."""
        op = cls(
            op_type=data["op_type"],
            position=data["position"],
            content=data["content"],
            client_id=data["client_id"],
            timestamp=data["timestamp"],
            operation_id=data["operation_id"]
        )
        if "version_vector" in data:
            op.version_vector = data["version_vector"]
        return op
    
    def __str__(self) -> str:
        return f"Op({self.op_type}, pos:{self.position}, content:{self.content})"


class OTEngine:
    """Main Operational Transform engine."""
    
    def __init__(self):
        """Initialize the OT engine."""
        self.document = ""
        self.history = []  # Operation history
        self.state_vector = {}  # Client state vectors
        
    def apply_operation(self, op: Operation) -> str:
        """
        Apply an operation to the current document.
        
        Args:
            op: The operation to apply
            
        Returns:
            Updated document state
        """
        if op.op_type == "insert":
            self.document = self.document[:op.position] + op.content + self.document[op.position:]
        elif op.op_type == "delete":
            length = 1 if op.content is None else len(op.content)
            self.document = self.document[:op.position] + self.document[op.position + length:]
        else:
            raise ValueError(f"Unknown operation type: {op.op_type}")
            
        self.history.append(op)
        self.state_vector[op.client_id] = op.operation_id
        
        return self.document
    
    def transform(self, op1: Operation, op2: Operation) -> Operation:
        """
        Transform operation op1 against op2.
        
        Args:
            op1: Operation to be transformed
            op2: Operation to transform against
            
        Returns:
            Transformed operation
        """
        # This is a simplified implementation
        # Real implementation would handle different operation types
        # and more complex scenarios
        
        if op1.op_type == "insert" and op2.op_type == "insert":
            # If op2 inserts at a position before or at op1's position,
            # op1's position needs to be adjusted
            if op2.position <= op1.position:
                return Operation(
                    op_type=op1.op_type,
                    position=op1.position + len(op2.content),
                    content=op1.content,
                    client_id=op1.client_id,
                    timestamp=op1.timestamp,
                    operation_id=op1.operation_id
                )
            
        elif op1.op_type == "delete" and op2.op_type == "insert":
            # If op2 inserts at a position before or at op1's position,
            # op1's position needs to be adjusted
            if op2.position <= op1.position:
                return Operation(
                    op_type=op1.op_type,
                    position=op1.position + len(op2.content),
                    content=op1.content,
                    client_id=op1.client_id,
                    timestamp=op1.timestamp,
                    operation_id=op1.operation_id
                )
            
        elif op1.op_type == "insert" and op2.op_type == "delete":
            # If op2 deletes a range that includes op1's position,
            # op1's position needs to be adjusted
            length = 1 if op2.content is None else len(op2.content)
            if op2.position < op1.position:
                if op2.position + length <= op1.position:
                    return Operation(
                        op_type=op1.op_type,
                        position=op1.position - length,
                        content=op1.content,
                        client_id=op1.client_id,
                        timestamp=op1.timestamp,
                        operation_id=op1.operation_id
                    )
                else:
                    # op1's insert position is within the deleted range
                    # In this case, the insert happens at the deletion point
                    return Operation(
                        op_type=op1.op_type,
                        position=op2.position,
                        content=op1.content,
                        client_id=op1.client_id,
                        timestamp=op1.timestamp,
                        operation_id=op1.operation_id
                    )
                    
        elif op1.op_type == "delete" and op2.op_type == "delete":
            # Handle overlapping deletes
            length1 = 1 if op1.content is None else len(op1.content)
            length2 = 1 if op2.content is None else len(op2.content)
            
            # If op2 deletes before op1
            if op2.position < op1.position:
                # Adjust op1's position
                new_pos = op1.position - length2
                if new_pos < 0:
                    new_pos = 0
                return Operation(
                    op_type=op1.op_type,
                    position=new_pos,
                    content=op1.content,
                    client_id=op1.client_id,
                    timestamp=op1.timestamp,
                    operation_id=op1.operation_id
                )
                
        # If no transformation needed, return the original operation
        return op1
    
    def transform_against_history(self, op: Operation) -> Operation:
        """
        Transform an operation against all operations in history
        that have been executed concurrently.
        
        Args:
            op: Operation to transform
            
        Returns:
            Transformed operation
        """
        transformed_op = op
        
        # Find all operations that are concurrent with the given operation
        for history_op in self.history:
            if self._is_concurrent(transformed_op, history_op):
                transformed_op = self.transform(transformed_op, history_op)
                
        return transformed_op
    
    def _is_concurrent(self, op1: Operation, op2: Operation) -> bool:
        """
        Check if two operations are concurrent.
        
        Args:
            op1: First operation
            op2: Second operation
            
        Returns:
            True if operations are concurrent, False otherwise
        """
        # This is a simplified implementation
        # In a real system, we would use state vectors or version vectors
        # to determine concurrent operations
        
        # Different clients and close timestamps indicate potential concurrency
        return (op1.client_id != op2.client_id and 
                abs(op1.timestamp - op2.timestamp) < 5)  # 5 second threshold
                
    def receive_operation(self, op: Operation) -> str:
        """
        Process received operation, transform it, and apply it.
        
        Args:
            op: Received operation
            
        Returns:
            Updated document
        """
        # Transform the operation against concurrent operations in history
        transformed_op = self.transform_against_history(op)
        
        # Apply the transformed operation
        return self.apply_operation(transformed_op)


class InterPlanetaryOTEngine(OTEngine):
    """
    Extended OT engine with features for high-latency inter-planetary communication.
    """
    
    def __init__(self, planet_id: str):
        """
        Initialize the inter-planetary OT engine.
        
        Args:
            planet_id: Identifier for the planet
        """
        super().__init__()
        self.planet_id = planet_id
        self.version_vector = {}  # For tracking causality across planets
        self.operation_buffer = {}  # Buffer for operations waiting for their causal operations
        
    def local_operation(self, op: Operation) -> str:
        """
        Handle a local operation (originated on this planet).
        
        Args:
            op: Local operation
            
        Returns:
            Updated document
        """
        # Update version vector for local operations
        if self.planet_id in self.version_vector:
            self.version_vector[self.planet_id] += 1
        else:
            self.version_vector[self.planet_id] = 1
            
        # Attach version vector to operation for causality tracking
        op.version_vector = self.version_vector.copy()
        
        # Apply operation locally
        return self.apply_operation(op)
    
    def remote_operation(self, op: Operation) -> Optional[str]:
        """
        Handle a remote operation (originated on another planet).
        
        Args:
            op: Remote operation
            
        Returns:
            Updated document or None if operation is buffered
        """
        # Check if all causal operations have been received
        if not self._check_causality(op):
            # Buffer the operation if we're missing causal operations
            self.operation_buffer[op.operation_id] = op
            logger.info(f"Operation {op.operation_id} buffered waiting for causal operations")
            return None
            
        # Process the operation
        result = self.receive_operation(op)
        
        # Update version vector
        for planet, version in op.version_vector.items():
            if planet not in self.version_vector or self.version_vector[planet] < version:
                self.version_vector[planet] = version
                
        # Check buffer for operations that might now be causally ready
        self._process_buffer()
        
        return result
    
    def _check_causality(self, op: Operation) -> bool:
        """
        Check if all causal operations for this operation have been received.
        
        Args:
            op: Operation to check
            
        Returns:
            True if all causal operations have been received, False otherwise
        """
        # Check each planet's version in the operation's vector
        for planet, version in op.version_vector.items():
            # Skip the operation's own planet
            if planet == op.planet_id:
                continue
                
            # Check if we have received all operations up to this version
            if planet not in self.version_vector or self.version_vector[planet] < version:
                return False
                
        return True
    
    def _process_buffer(self) -> None:
        """Process buffered operations that are now causally ready."""
        processed_ids = []
        
        for op_id, op in self.operation_buffer.items():
            if self._check_causality(op):
                # Process the operation
                self.receive_operation(op)
                processed_ids.append(op_id)
                
                # Update version vector
                for planet, version in op.version_vector.items():
                    if planet not in self.version_vector or self.version_vector[planet] < version:
                        self.version_vector[planet] = version
        
        # Remove processed operations from buffer
        for op_id in processed_ids:
            del self.operation_buffer[op_id]
            
        # If we processed any operations, check buffer again
        if processed_ids:
            self._process_buffer()
