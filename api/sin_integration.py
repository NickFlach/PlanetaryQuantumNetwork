"""
SIN Integration Module

This module provides integration with the SIN application, allowing
the OT network to be used as a collaborative backend for SIN.

Key components:
- SIN API client
- Data mapping between OT and SIN formats
- Synchronization mechanisms
- Authentication and authorization
"""

import os
import json
import time
import hmac
import hashlib
import base64
import logging
import aiohttp
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple

from core.ot_engine import Operation, InterPlanetaryOTEngine
from crypto.zkp import ZKDocumentProof
from crypto.quantum_resistant import QuantumResistantCrypto

logger = logging.getLogger(__name__)

class SINClient:
    """Client for interacting with the SIN application."""
    
    def __init__(self, 
                 api_endpoint: str = None,
                 api_key: str = None,
                 verify_ssl: bool = True):
        """
        Initialize the SIN client.
        
        Args:
            api_endpoint: URL of the SIN API
            api_key: API key for authentication
            verify_ssl: Whether to verify SSL certificates
        """
        self.api_endpoint = api_endpoint or os.getenv("SIN_API_ENDPOINT", "http://localhost:8080")
        self.api_key = api_key or os.getenv("SIN_API_KEY", "test-api-key")
        self.verify_ssl = verify_ssl
        self.session = None
        
    async def __aenter__(self):
        """Create a session when entering context."""
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close the session when exiting context."""
        if self.session:
            await self.session.close()
            self.session = None
            
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get the current session or create a new one."""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            )
        return self.session
        
    async def get_document(self, document_id: str) -> Dict[str, Any]:
        """
        Get a document from SIN.
        
        Args:
            document_id: ID of the document
            
        Returns:
            Document data
        """
        session = await self._get_session()
        url = f"{self.api_endpoint}/documents/{document_id}"
        
        async with session.get(url, ssl=self.verify_ssl) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to get document {document_id}: {response.status} - {error_text}")
                response.raise_for_status()
                
            return await response.json()
            
    async def update_document(self, document_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a document in SIN.
        
        Args:
            document_id: ID of the document
            updates: Updates to apply
            
        Returns:
            Updated document data
        """
        session = await self._get_session()
        url = f"{self.api_endpoint}/documents/{document_id}"
        
        async with session.patch(url, json=updates, ssl=self.verify_ssl) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to update document {document_id}: {response.status} - {error_text}")
                response.raise_for_status()
                
            return await response.json()
            
    async def create_document(self, document_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new document in SIN.
        
        Args:
            document_data: Document data
            
        Returns:
            Created document data
        """
        session = await self._get_session()
        url = f"{self.api_endpoint}/documents"
        
        async with session.post(url, json=document_data, ssl=self.verify_ssl) as response:
            if response.status != 201:
                error_text = await response.text()
                logger.error(f"Failed to create document: {response.status} - {error_text}")
                response.raise_for_status()
                
            return await response.json()
            
    async def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Get a user from SIN.
        
        Args:
            user_id: ID of the user
            
        Returns:
            User data
        """
        session = await self._get_session()
        url = f"{self.api_endpoint}/users/{user_id}"
        
        async with session.get(url, ssl=self.verify_ssl) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to get user {user_id}: {response.status} - {error_text}")
                response.raise_for_status()
                
            return await response.json()
            
    async def create_collaboration(self, document_id: str, 
                                  users: List[str]) -> Dict[str, Any]:
        """
        Create a collaboration in SIN.
        
        Args:
            document_id: ID of the document
            users: List of user IDs
            
        Returns:
            Collaboration data
        """
        session = await self._get_session()
        url = f"{self.api_endpoint}/documents/{document_id}/collaborations"
        
        payload = {
            "users": users
        }
        
        async with session.post(url, json=payload, ssl=self.verify_ssl) as response:
            if response.status != 201:
                error_text = await response.text()
                logger.error(f"Failed to create collaboration: {response.status} - {error_text}")
                response.raise_for_status()
                
            return await response.json()


class SINIntegration:
    """Integration between the OT network and SIN application."""
    
    def __init__(self, 
                 ot_engine: InterPlanetaryOTEngine,
                 sin_client: Optional[SINClient] = None,
                 sync_interval: int = 30):
        """
        Initialize the SIN integration.
        
        Args:
            ot_engine: OT engine instance
            sin_client: SIN client instance
            sync_interval: Interval for synchronizing with SIN (seconds)
        """
        self.ot_engine = ot_engine
        self.sin_client = sin_client or SINClient()
        self.sync_interval = sync_interval
        
        # Document mappings: document_id -> last_synced_version
        self.documents: Dict[str, Dict[str, Any]] = {}
        
        # Lock for thread safety
        self.lock = asyncio.Lock()
        
        # Running flag
        self.running = False
        
        # Authentication tokens: user_id -> auth_token
        self.auth_tokens: Dict[str, str] = {}
        
    async def start(self) -> None:
        """Start the SIN integration."""
        if self.running:
            return
            
        self.running = True
        
        # Start sync task
        asyncio.create_task(self._sync_task())
        
        logger.info("SIN integration started")
        
    async def stop(self) -> None:
        """Stop the SIN integration."""
        if not self.running:
            return
            
        self.running = False
        
        logger.info("SIN integration stopped")
        
    async def _sync_task(self) -> None:
        """Periodically synchronize with SIN."""
        while self.running:
            try:
                await self._sync_all_documents()
            except Exception as e:
                logger.error(f"Error during SIN synchronization: {e}")
                
            await asyncio.sleep(self.sync_interval)
            
    async def _sync_all_documents(self) -> None:
        """Synchronize all tracked documents with SIN."""
        async with self.lock:
            for document_id, doc_info in list(self.documents.items()):
                try:
                    await self._sync_document(document_id)
                except Exception as e:
                    logger.error(f"Error synchronizing document {document_id}: {e}")
                    
    async def _sync_document(self, document_id: str) -> None:
        """
        Synchronize a specific document with SIN.
        
        Args:
            document_id: ID of the document to synchronize
        """
        async with self.lock:
            if document_id not in self.documents:
                logger.warning(f"Document {document_id} not tracked")
                return
                
            doc_info = self.documents[document_id]
            
        # Get current document from SIN
        try:
            async with self.sin_client as client:
                sin_doc = await client.get_document(document_id)
                
            # Check if document has been updated in SIN
            if sin_doc["version"] > doc_info["last_synced_version"]:
                # Document was updated in SIN, apply changes to OT
                await self._apply_sin_changes(document_id, sin_doc)
                
            else:
                # Document may have been updated in OT, apply changes to SIN
                await self._apply_ot_changes(document_id, sin_doc)
                
        except Exception as e:
            logger.error(f"Error synchronizing document {document_id}: {e}")
            
    async def _apply_sin_changes(self, document_id: str, sin_doc: Dict[str, Any]) -> None:
        """
        Apply changes from SIN to the OT engine.
        
        Args:
            document_id: ID of the document
            sin_doc: Document data from SIN
        """
        async with self.lock:
            if document_id not in self.documents:
                logger.warning(f"Document {document_id} not tracked")
                return
                
            doc_info = self.documents[document_id]
            
            # Extract content from SIN document
            content = sin_doc.get("content", "")
            
            # If content has changed since last sync
            if content != doc_info.get("last_content", ""):
                # Replace content in OT engine
                operations = self._diff_to_operations(doc_info.get("last_content", ""), content)
                
                for op in operations:
                    self.ot_engine.local_operation(op)
                    
                # Update tracking info
                doc_info["last_content"] = content
                doc_info["last_synced_version"] = sin_doc["version"]
                
    async def _apply_ot_changes(self, document_id: str, sin_doc: Dict[str, Any]) -> None:
        """
        Apply changes from OT engine to SIN.
        
        Args:
            document_id: ID of the document
            sin_doc: Current document data from SIN
        """
        async with self.lock:
            if document_id not in self.documents:
                logger.warning(f"Document {document_id} not tracked")
                return
                
            doc_info = self.documents[document_id]
            
            # Get current content from OT engine
            current_content = self.ot_engine.document
            
            # If content has changed since last sync
            if current_content != doc_info.get("last_content", ""):
                # Update SIN document
                async with self.sin_client as client:
                    updates = {
                        "content": current_content,
                        "previous_version": sin_doc["version"]
                    }
                    
                    try:
                        updated_doc = await client.update_document(document_id, updates)
                        
                        # Update tracking info
                        doc_info["last_content"] = current_content
                        doc_info["last_synced_version"] = updated_doc["version"]
                        
                    except aiohttp.ClientResponseError as e:
                        if e.status == 409:
                            # Conflict, refresh and try again
                            logger.warning(f"Conflict updating document {document_id}, will retry")
                            await self._sync_document(document_id)
                        else:
                            raise
                            
    def _diff_to_operations(self, old_content: str, new_content: str) -> List[Operation]:
        """
        Convert a content diff to a list of OT operations.
        
        Args:
            old_content: Old content
            new_content: New content
            
        Returns:
            List of operations to transform old_content into new_content
        """
        # Simple implementation - real implementation would use a proper diff algorithm
        operations = []
        
        # Delete old content
        if old_content:
            operations.append(Operation(
                op_type="delete",
                position=0,
                content=old_content
            ))
            
        # Insert new content
        if new_content:
            operations.append(Operation(
                op_type="insert",
                position=0,
                content=new_content
            ))
            
        return operations
        
    async def track_document(self, document_id: str) -> None:
        """
        Start tracking a document.
        
        Args:
            document_id: ID of the document to track
        """
        async with self.lock:
            if document_id in self.documents:
                logger.info(f"Document {document_id} already tracked")
                return
                
        # Get document from SIN
        async with self.sin_client as client:
            sin_doc = await client.get_document(document_id)
            
        async with self.lock:
            # Initialize OT engine with document content
            # Clear existing content
            if self.ot_engine.document:
                self.ot_engine.apply_operation(Operation(
                    op_type="delete",
                    position=0,
                    content=self.ot_engine.document
                ))
                
            # Insert new content
            content = sin_doc.get("content", "")
            if content:
                self.ot_engine.apply_operation(Operation(
                    op_type="insert",
                    position=0,
                    content=content
                ))
                
            # Start tracking
            self.documents[document_id] = {
                "last_content": content,
                "last_synced_version": sin_doc["version"]
            }
            
        logger.info(f"Started tracking document {document_id}")
        
    async def untrack_document(self, document_id: str) -> None:
        """
        Stop tracking a document.
        
        Args:
            document_id: ID of the document to stop tracking
        """
        async with self.lock:
            if document_id not in self.documents:
                logger.info(f"Document {document_id} not tracked")
                return
                
            del self.documents[document_id]
            
        logger.info(f"Stopped tracking document {document_id}")
        
    async def create_document(self, content: str, metadata: Dict[str, Any]) -> str:
        """
        Create a new document in SIN.
        
        Args:
            content: Initial document content
            metadata: Document metadata
            
        Returns:
            ID of the created document
        """
        # Create document in SIN
        async with self.sin_client as client:
            doc_data = {
                "content": content,
                **metadata
            }
            
            sin_doc = await client.create_document(doc_data)
            
        document_id = sin_doc["id"]
        
        # Start tracking
        await self.track_document(document_id)
        
        return document_id
        
    async def apply_operation(self, document_id: str, operation: Operation) -> None:
        """
        Apply an operation to a document.
        
        Args:
            document_id: ID of the document
            operation: Operation to apply
        """
        async with self.lock:
            if document_id not in self.documents:
                raise ValueError(f"Document {document_id} not tracked")
                
            # Apply operation to OT engine
            result = self.ot_engine.local_operation(operation)
            
            # Update tracking info
            self.documents[document_id]["last_content"] = result
            
    async def authenticate_user(self, user_id: str, password: str) -> Optional[str]:
        """
        Authenticate a user with SIN.
        
        Args:
            user_id: User ID
            password: User password
            
        Returns:
            Auth token if successful, None otherwise
        """
        # Create a hash of the password for authentication
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        # Authenticate with SIN
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.sin_client.api_endpoint}/auth/login"
                
                auth_data = {
                    "user_id": user_id,
                    "password_hash": password_hash
                }
                
                async with session.post(url, json=auth_data, ssl=self.sin_client.verify_ssl) as response:
                    if response.status != 200:
                        return None
                        
                    data = await response.json()
                    token = data.get("token")
                    
                    if token:
                        # Store token
                        self.auth_tokens[user_id] = token
                        return token
                        
                    return None
                    
        except Exception as e:
            logger.error(f"Error authenticating user {user_id}: {e}")
            return None
            
    def generate_auth_proof(self, user_id: str, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Generate a zero-knowledge proof of document access.
        
        Args:
            user_id: User ID
            document_id: Document ID
            
        Returns:
            Proof data if successful, None otherwise
        """
        if user_id not in self.auth_tokens:
            return None
            
        token = self.auth_tokens[user_id]
        
        # Create a document-specific key
        document_bytes = document_id.encode()
        user_key = hashlib.sha256(token.encode()).digest()
        
        # Generate proof
        try:
            proof = ZKDocumentProof.prove_document_access(document_bytes, user_key)
            return proof
        except Exception as e:
            logger.error(f"Error generating auth proof: {e}")
            return None
            
    def verify_auth_proof(self, document_id: str, proof: Dict[str, Any]) -> bool:
        """
        Verify a zero-knowledge proof of document access.
        
        Args:
            document_id: Document ID
            proof: Proof data
            
        Returns:
            True if the proof is valid, False otherwise
        """
        doc_hash = hashlib.sha256(document_id.encode()).hexdigest()
        
        # Custom verification function for document key
        def verify_key(proof_data: bytes, signature: bytes) -> bool:
            # In a real implementation, this would check against SIN API
            # For now, we'll just validate the signature format
            return len(signature) == 64
            
        # Verify proof
        try:
            result = ZKDocumentProof.verify_document_access(proof, doc_hash, verify_key)
            return result
        except Exception as e:
            logger.error(f"Error verifying auth proof: {e}")
            return False


class SINDocumentSession:
    """Session for editing a SIN document through the OT network."""
    
    def __init__(self, 
                 document_id: str,
                 user_id: str,
                 integration: SINIntegration):
        """
        Initialize a document session.
        
        Args:
            document_id: ID of the document
            user_id: ID of the user
            integration: SIN integration instance
        """
        self.document_id = document_id
        self.user_id = user_id
        self.integration = integration
        self.authorized = False
        
    async def authorize(self, auth_proof: Dict[str, Any]) -> bool:
        """
        Authorize the session.
        
        Args:
            auth_proof: Proof of authorization
            
        Returns:
            True if authorization successful, False otherwise
        """
        # Verify proof
        if self.integration.verify_auth_proof(self.document_id, auth_proof):
            self.authorized = True
            return True
        else:
            return False
            
    async def apply_operation(self, operation: Operation) -> bool:
        """
        Apply an operation to the document.
        
        Args:
            operation: Operation to apply
            
        Returns:
            True if successful, False otherwise
        """
        if not self.authorized:
            return False
            
        try:
            # Set client ID to user ID
            operation.client_id = self.user_id
            
            # Apply operation
            await self.integration.apply_operation(self.document_id, operation)
            return True
        except Exception as e:
            logger.error(f"Error applying operation: {e}")
            return False
            
    async def get_content(self) -> Optional[str]:
        """
        Get the current document content.
        
        Returns:
            Current document content
        """
        if not self.authorized:
            return None
            
        async with self.integration.lock:
            return self.integration.ot_engine.document
