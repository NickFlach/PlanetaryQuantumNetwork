"""
Operational Transform Tests

This module contains tests for the core OT engine functionality.
"""

import unittest
import asyncio
import random
import string
from typing import List

from ..core.ot_engine import Operation, OTEngine, InterPlanetaryOTEngine

class TestOTEngine(unittest.TestCase):
    """Test cases for the OT engine."""
    
    def setUp(self):
        """Set up test environment."""
        self.engine = OTEngine()
        
    def test_insert_operation(self):
        """Test insert operation."""
        # Insert "Hello" at position 0
        op = Operation(op_type="insert", position=0, content="Hello")
        result = self.engine.apply_operation(op)
        
        self.assertEqual(result, "Hello")
        self.assertEqual(self.engine.document, "Hello")
        
    def test_delete_operation(self):
        """Test delete operation."""
        # Insert "Hello World" at position 0
        op1 = Operation(op_type="insert", position=0, content="Hello World")
        self.engine.apply_operation(op1)
        
        # Delete "World" (positions 6-11)
        op2 = Operation(op_type="delete", position=6, content="World")
        result = self.engine.apply_operation(op2)
        
        self.assertEqual(result, "Hello ")
        self.assertEqual(self.engine.document, "Hello ")
        
    def test_transform(self):
        """Test operation transformation."""
        # Create two operations:
        # 1. Insert "X" at position 3
        # 2. Insert "Y" at position 2
        op1 = Operation(op_type="insert", position=3, content="X")
        op2 = Operation(op_type="insert", position=2, content="Y")
        
        # Transform op1 against op2
        transformed_op1 = self.engine.transform(op1, op2)
        
        # Since op2 inserts at position 2, op1's position should be adjusted to 4
        self.assertEqual(transformed_op1.position, 4)
        self.assertEqual(transformed_op1.content, "X")
        
    def test_concurrent_operations(self):
        """Test concurrent operations."""
        # Start with "ABCDE"
        self.engine.apply_operation(Operation(op_type="insert", position=0, content="ABCDE"))
        
        # Two concurrent operations:
        # 1. Insert "X" at position 1
        # 2. Insert "Y" at position 3
        op1 = Operation(op_type="insert", position=1, content="X", client_id="client1", timestamp=1000)
        op2 = Operation(op_type="insert", position=3, content="Y", client_id="client2", timestamp=1000)
        
        # Apply op1
        self.engine.apply_operation(op1)
        # Document: "AXBCDE"
        
        # Transform op2 against op1
        transformed_op2 = self.engine.transform_against_history(op2)
        
        # Since op1 inserts at position 1, op2's position should be adjusted to 4
        self.assertEqual(transformed_op2.position, 4)
        
        # Apply transformed op2
        self.engine.apply_operation(transformed_op2)
        
        # Final document should be "AXBYCDE"
        self.assertEqual(self.engine.document, "AXBYCDE")
        
    def test_delete_insert_operations(self):
        """Test delete followed by insert operations."""
        # Start with "ABCDE"
        self.engine.apply_operation(Operation(op_type="insert", position=0, content="ABCDE"))
        
        # Delete "CD" (positions 2-4)
        self.engine.apply_operation(Operation(op_type="delete", position=2, content="CD"))
        # Document: "ABE"
        
        # Insert "XYZ" at position 2
        self.engine.apply_operation(Operation(op_type="insert", position=2, content="XYZ"))
        
        # Final document should be "ABEXY"
        self.assertEqual(self.engine.document, "ABXYZE")


class TestInterPlanetaryOTEngine(unittest.TestCase):
    """Test cases for the inter-planetary OT engine."""
    
    def setUp(self):
        """Set up test environment."""
        self.earth_engine = InterPlanetaryOTEngine("earth")
        self.mars_engine = InterPlanetaryOTEngine("mars")
        
    def test_local_operation(self):
        """Test local operation processing."""
        # Create a local operation on Earth
        op = Operation(op_type="insert", position=0, content="Hello from Earth", client_id="earth-1")
        
        # Apply it locally
        result = self.earth_engine.local_operation(op)
        
        self.assertEqual(result, "Hello from Earth")
        self.assertEqual(self.earth_engine.document, "Hello from Earth")
        
        # Check that version vector was updated
        self.assertIn("earth", self.earth_engine.version_vector)
        self.assertEqual(self.earth_engine.version_vector["earth"], 1)
        
    def test_remote_operation(self):
        """Test remote operation processing."""
        # Create an operation on Earth
        earth_op = Operation(op_type="insert", position=0, content="Hello from Earth", client_id="earth-1")
        earth_op.version_vector = {"earth": 1}
        earth_op.planet_id = "earth"
        
        # Apply it on Mars
        result = self.mars_engine.remote_operation(earth_op)
        
        self.assertEqual(result, "Hello from Earth")
        self.assertEqual(self.mars_engine.document, "Hello from Earth")
        
        # Check that version vector was updated
        self.assertIn("earth", self.mars_engine.version_vector)
        self.assertEqual(self.mars_engine.version_vector["earth"], 1)
        
    def test_causal_operations(self):
        """Test causally dependent operations."""
        # Create an operation on Earth
        earth_op1 = Operation(op_type="insert", position=0, content="Hello from Earth", client_id="earth-1")
        earth_op1.version_vector = {"earth": 1}
        earth_op1.planet_id = "earth"
        
        # Apply it on Mars
        self.mars_engine.remote_operation(earth_op1)
        
        # Create another operation on Earth that depends on the first
        earth_op2 = Operation(op_type="insert", position=16, content="!", client_id="earth-1")
        earth_op2.version_vector = {"earth": 2}
        earth_op2.planet_id = "earth"
        
        # Apply it on Mars
        self.mars_engine.remote_operation(earth_op2)
        
        self.assertEqual(self.mars_engine.document, "Hello from Earth!")


class TestLargeDocumentEditing(unittest.TestCase):
    """Test cases for editing large documents."""
    
    def setUp(self):
        """Set up test environment."""
        self.engine = OTEngine()
        
    def test_many_operations(self):
        """Test applying many operations to a document."""
        # Create a large initial document
        initial_text = "This is a test document for evaluating the performance of the OT engine."
        self.engine.apply_operation(Operation(op_type="insert", position=0, content=initial_text))
        
        # Apply 100 random operations
        for i in range(100):
            op_type = random.choice(["insert", "delete"])
            
            if op_type == "insert":
                position = random.randint(0, len(self.engine.document))
                content = ''.join(random.choices(string.ascii_letters + string.digits + " ", k=random.randint(1, 10)))
                op = Operation(op_type="insert", position=position, content=content)
            else:
                if len(self.engine.document) > 0:
                    position = random.randint(0, len(self.engine.document) - 1)
                    length = random.randint(1, min(10, len(self.engine.document) - position))
                    content = self.engine.document[position:position+length]
                    op = Operation(op_type="delete", position=position, content=content)
                else:
                    # If document is empty, insert something
                    op = Operation(op_type="insert", position=0, content="New content")
                    
            self.engine.apply_operation(op)
            
        # Verify that we can still edit the document
        final_op = Operation(op_type="insert", position=0, content="Final: ")
        result = self.engine.apply_operation(final_op)
        
        self.assertTrue(result.startswith("Final: "))


class TestConcurrencyControl(unittest.TestCase):
    """Test cases for concurrency control mechanisms."""
    
    def setUp(self):
        """Set up test environment."""
        self.earth_engine = InterPlanetaryOTEngine("earth")
        self.mars_engine = InterPlanetaryOTEngine("mars")
        self.moon_engine = InterPlanetaryOTEngine("moon")
        
    def test_convergence(self):
        """Test that concurrent operations converge to the same state."""
        # Initial content
        initial_op = Operation(op_type="insert", position=0, content="Initial content")
        initial_op.version_vector = {"earth": 1}
        initial_op.planet_id = "earth"
        
        # Apply to all engines
        self.earth_engine.local_operation(initial_op)
        self.mars_engine.remote_operation(initial_op)
        self.moon_engine.remote_operation(initial_op)
        
        # Concurrent operations on each planet
        earth_op = Operation(op_type="insert", position=8, content="Earth ", client_id="earth-1")
        earth_op.version_vector = {"earth": 2}
        earth_op.planet_id = "earth"
        
        mars_op = Operation(op_type="insert", position=0, content="Mars says: ", client_id="mars-1")
        mars_op.version_vector = {"mars": 1}
        mars_op.planet_id = "mars"
        
        moon_op = Operation(op_type="delete", position=8, content="content", client_id="moon-1")
        moon_op.version_vector = {"moon": 1}
        moon_op.planet_id = "moon"
        
        # Apply locally
        self.earth_engine.local_operation(earth_op)
        self.mars_engine.local_operation(mars_op)
        self.moon_engine.local_operation(moon_op)
        
        # Apply remote operations to all engines
        self.earth_engine.remote_operation(mars_op)
        self.earth_engine.remote_operation(moon_op)
        
        self.mars_engine.remote_operation(earth_op)
        self.mars_engine.remote_operation(moon_op)
        
        self.moon_engine.remote_operation(earth_op)
        self.moon_engine.remote_operation(mars_op)
        
        # All engines should have the same document state
        self.assertEqual(self.earth_engine.document, self.mars_engine.document)
        self.assertEqual(self.mars_engine.document, self.moon_engine.document)


if __name__ == '__main__':
    unittest.main()
