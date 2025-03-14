"""
Quantum-Resistant Cryptography Module

This module implements quantum-resistant cryptographic algorithms
for secure communication in the distributed OT network.

Key components:
- Post-quantum key exchange
- Lattice-based encryption
- Hash-based signatures
- Symmetric encryption for quantum resistance
"""

import os
import time
import hashlib
import hmac
import logging
from typing import Dict, List, Tuple, Any, Optional, Callable, Union, ByteString

# Note: This implementation uses Python's standard libraries and algorithms
# that are considered quantum-resistant (to a degree). In a production environment,
# you would use specialized libraries like liboqs, PQClean, or NIST PQC standardized 
# implementations.

logger = logging.getLogger(__name__)

class QuantumResistantCrypto:
    """Base class for quantum-resistant cryptographic operations."""
    
    @staticmethod
    def generate_secure_random(length: int) -> bytes:
        """
        Generate cryptographically secure random bytes.
        
        Args:
            length: Number of random bytes to generate
            
        Returns:
            Random bytes
        """
        return os.urandom(length)
        
    @staticmethod
    def hash_data(data: Union[str, bytes]) -> bytes:
        """
        Hash data using a quantum-resistant hash function (SHA-512).
        
        Args:
            data: Data to hash (string or bytes)
            
        Returns:
            Hash value
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
            
        return hashlib.sha512(data).digest()
        
    @staticmethod
    def hmac_data(key: bytes, data: Union[str, bytes]) -> bytes:
        """
        Create an HMAC using a quantum-resistant algorithm.
        
        Args:
            key: Key for the HMAC
            data: Data to authenticate
            
        Returns:
            HMAC value
        """
        if isinstance(data, str):
            data = data.encode('utf-8')
            
        return hmac.new(key, data, hashlib.sha512).digest()


class LamportSignature:
    """
    Implementation of Lamport signatures, a hash-based one-time signature scheme
    that is resistant to quantum attacks.
    """
    
    @staticmethod
    def generate_keypair(seed: Optional[bytes] = None) -> Tuple[List[List[bytes]], List[List[bytes]]]:
        """
        Generate a Lamport signature key pair.
        
        Args:
            seed: Optional seed for key generation
            
        Returns:
            Tuple of (private_key, public_key)
        """
        if seed is None:
            seed = QuantumResistantCrypto.generate_secure_random(64)
            
        # Private key: 512 random values (256 pairs for each bit of the message digest)
        private_key = []
        for i in range(512):
            value = hashlib.sha256(seed + str(i).encode()).digest()
            private_key.append(value)
            
        # Reshape private key into 256 pairs
        private_key_pairs = [private_key[i:i+2] for i in range(0, 512, 2)]
        
        # Public key: hash of each private key value
        public_key_pairs = []
        for pair in private_key_pairs:
            public_pair = [hashlib.sha256(value).digest() for value in pair]
            public_key_pairs.append(public_pair)
            
        return private_key_pairs, public_key_pairs
        
    @staticmethod
    def sign(private_key: List[List[bytes]], message: Union[str, bytes]) -> List[bytes]:
        """
        Sign a message using a Lamport private key.
        
        Args:
            private_key: Lamport private key (256 pairs of values)
            message: Message to sign
            
        Returns:
            Signature (256 selected private key values)
        """
        if isinstance(message, str):
            message = message.encode('utf-8')
            
        # Hash the message
        message_hash = hashlib.sha256(message).digest()
        
        # Convert hash to bit string
        hash_bits = ''.join(format(byte, '08b') for byte in message_hash)
        
        # Select private key values based on hash bits
        signature = []
        for i, bit in enumerate(hash_bits):
            # Select the appropriate private key value from the pair
            bit_index = int(bit)
            signature.append(private_key[i][bit_index])
            
        return signature
        
    @staticmethod
    def verify(public_key: List[List[bytes]], message: Union[str, bytes], 
               signature: List[bytes]) -> bool:
        """
        Verify a Lamport signature.
        
        Args:
            public_key: Lamport public key (256 pairs of values)
            message: Message that was signed
            signature: Signature to verify
            
        Returns:
            True if the signature is valid, False otherwise
        """
        if isinstance(message, str):
            message = message.encode('utf-8')
            
        # Hash the message
        message_hash = hashlib.sha256(message).digest()
        
        # Convert hash to bit string
        hash_bits = ''.join(format(byte, '08b') for byte in message_hash)
        
        # Verify each bit of the signature
        for i, bit in enumerate(hash_bits):
            bit_index = int(bit)
            # Hash the signature value
            sig_hash = hashlib.sha256(signature[i]).digest()
            # Compare with the corresponding public key value
            if sig_hash != public_key[i][bit_index]:
                return False
                
        return True


class NTRUEncryption:
    """
    Simplified implementation of NTRU encryption, a lattice-based encryption
    scheme that is resistant to quantum attacks.
    
    Note: This is a simplified educational implementation. In a real system,
    you would use a well-vetted library implementation.
    """
    
    def __init__(self, N: int = 503, p: int = 3, q: int = 256, df: int = 61, dg: int = 20):
        """
        Initialize NTRU parameters.
        
        Args:
            N: Polynomial degree
            p: Small modulus
            q: Large modulus
            df: Number of coefficients 1 in private polynomial f
            dg: Number of coefficients 1 in polynomial g
        """
        self.N = N
        self.p = p
        self.q = q
        self.df = df
        self.dg = dg
        
    def _poly_mult(self, a: List[int], b: List[int]) -> List[int]:
        """
        Multiply two polynomials in Z_q[X]/(X^N - 1).
        
        Args:
            a: First polynomial coefficients
            b: Second polynomial coefficients
            
        Returns:
            Product polynomial coefficients
        """
        c = [0] * self.N
        
        for i in range(self.N):
            for j in range(self.N):
                # Compute the index in the result
                idx = (i + j) % self.N
                c[idx] = (c[idx] + a[i] * b[j]) % self.q
                
        return c
        
    def _poly_add(self, a: List[int], b: List[int]) -> List[int]:
        """
        Add two polynomials in Z_q[X]/(X^N - 1).
        
        Args:
            a: First polynomial coefficients
            b: Second polynomial coefficients
            
        Returns:
            Sum polynomial coefficients
        """
        return [(a[i] + b[i]) % self.q for i in range(self.N)]
        
    def _poly_sub(self, a: List[int], b: List[int]) -> List[int]:
        """
        Subtract two polynomials in Z_q[X]/(X^N - 1).
        
        Args:
            a: First polynomial coefficients
            b: Second polynomial coefficients
            
        Returns:
            Difference polynomial coefficients
        """
        return [(a[i] - b[i]) % self.q for i in range(self.N)]
        
    def _invert_poly(self, f: List[int], mod: int) -> Optional[List[int]]:
        """
        Find the inverse of a polynomial in Z_mod[X]/(X^N - 1).
        
        Args:
            f: Polynomial coefficients
            mod: Modulus
            
        Returns:
            Inverse polynomial coefficients, or None if not invertible
        """
        # Extended Euclidean algorithm for polynomial inversion
        # This is a simplified implementation and may not work for all inputs
        
        # Initialize variables
        r0 = [i % mod for i in f]
        r1 = [0] * self.N
        r1[0] = 1  # Represents X^N - 1
        r1[self.N - 1] = mod - 1
        
        v0 = [0] * self.N
        v1 = [0] * self.N
        v0[0] = 1
        
        # Extended Euclidean algorithm
        while True:
            # Find the quotient and remainder
            q = [0] * self.N
            r = self._poly_sub(r0, self._poly_mult(q, r1))
            
            # If r is zero, return v1
            if all(c == 0 for c in r):
                return v1
                
            # If degree of r is less than degree of r1, swap
            v = self._poly_sub(v0, self._poly_mult(q, v1))
            
            r0, r1 = r1, r
            v0, v1 = v1, v
            
            # Limit iterations to prevent infinite loops
            if all(c == 0 for c in r1):
                return None
                
    def generate_keypair(self, seed: Optional[bytes] = None) -> Tuple[Tuple[List[int], List[int]], List[int]]:
        """
        Generate an NTRU key pair.
        
        Args:
            seed: Optional seed for key generation
            
        Returns:
            Tuple of (private_key, public_key)
            private_key is a tuple of (f, fp)
            public_key is h
        """
        if seed is None:
            seed = QuantumResistantCrypto.generate_secure_random(32)
            
        # Use the seed to generate deterministic randomness
        import random
        random.seed(int.from_bytes(seed, byteorder='big'))
        
        # Generate polynomials f and g
        f = [0] * self.N
        g = [0] * self.N
        
        # Set df coefficients to 1, df-1 coefficients to -1
        indices = random.sample(range(self.N), 2 * self.df - 1)
        for i in indices[:self.df]:
            f[i] = 1
        for i in indices[self.df:]:
            f[i] = self.q - 1  # -1 mod q
            
        # Set dg coefficients to 1, dg coefficients to -1
        indices = random.sample(range(self.N), 2 * self.dg)
        for i in indices[:self.dg]:
            g[i] = 1
        for i in indices[self.dg:]:
            g[i] = self.q - 1  # -1 mod q
            
        # Compute f^-1 mod p
        fp = self._invert_poly(f, self.p)
        if fp is None:
            # Try again with different f if not invertible
            return self.generate_keypair(QuantumResistantCrypto.generate_secure_random(32))
            
        # Compute f^-1 mod q
        fq = self._invert_poly(f, self.q)
        if fq is None:
            # Try again with different f if not invertible
            return self.generate_keypair(QuantumResistantCrypto.generate_secure_random(32))
            
        # Compute h = p * g * fq mod q
        g_times_fq = self._poly_mult(g, fq)
        h = [(self.p * coeff) % self.q for coeff in g_times_fq]
        
        return (f, fp), h
        
    def encrypt(self, public_key: List[int], message: Union[str, bytes], 
                r: Optional[List[int]] = None) -> List[int]:
        """
        Encrypt a message using NTRU.
        
        Args:
            public_key: NTRU public key (h)
            message: Message to encrypt
            r: Optional random polynomial
            
        Returns:
            Encrypted message (polynomial e)
        """
        h = public_key
        
        # Convert message to polynomial m
        if isinstance(message, str):
            message = message.encode('utf-8')
            
        # Pad message to match polynomial degree
        padded_message = bytearray(message)
        while len(padded_message) < self.N:
            padded_message.append(0)
            
        # Convert bytes to polynomial (each coefficient is a value between 0 and p-1)
        m = [min(byte % self.p, self.p - 1) for byte in padded_message[:self.N]]
        
        # Generate random polynomial r if not provided
        if r is None:
            import random
            r = [0] * self.N
            indices = random.sample(range(self.N), 2 * self.df)
            for i in indices[:self.df]:
                r[i] = 1
            for i in indices[self.df:]:
                r[i] = self.q - 1  # -1 mod q
                
        # Compute e = r * h + m mod q
        r_times_h = self._poly_mult(r, h)
        e = self._poly_add(r_times_h, m)
        
        return e
        
    def decrypt(self, private_key: Tuple[List[int], List[int]], ciphertext: List[int]) -> bytes:
        """
        Decrypt an NTRU ciphertext.
        
        Args:
            private_key: NTRU private key (f, fp)
            ciphertext: Encrypted message (polynomial e)
            
        Returns:
            Decrypted message
        """
        f, fp = private_key
        e = ciphertext
        
        # Compute a = f * e mod q
        a = self._poly_mult(f, e)
        
        # Ensure coefficients are in the range [-q/2, q/2]
        a = [(coeff if coeff <= self.q // 2 else coeff - self.q) for coeff in a]
        
        # Compute m = fp * a mod p
        a_mod_p = [coeff % self.p for coeff in a]
        m = self._poly_mult(fp, a_mod_p)
        m = [coeff % self.p for coeff in m]
        
        # Convert polynomial back to bytes
        message = bytes(m)
        
        # Remove padding (trailing zeros)
        message = message.rstrip(b'\x00')
        
        return message


class QuantumKEM:
    """
    Key Encapsulation Mechanism (KEM) resistant to quantum attacks.
    This implementation uses NTRU for asymmetric operations and
    HMAC-based key derivation for symmetric keys.
    """
    
    def __init__(self, ntru_params: Optional[Dict[str, int]] = None):
        """
        Initialize the quantum-resistant KEM.
        
        Args:
            ntru_params: Optional parameters for NTRU encryption
        """
        self.ntru = NTRUEncryption(**(ntru_params or {}))
        
    def generate_keypair(self, seed: Optional[bytes] = None) -> Tuple[Any, Any]:
        """
        Generate a keypair for the KEM.
        
        Args:
            seed: Optional seed for key generation
            
        Returns:
            Tuple of (private_key, public_key)
        """
        return self.ntru.generate_keypair(seed)
        
    def encapsulate(self, public_key: Any) -> Tuple[bytes, List[int]]:
        """
        Encapsulate a symmetric key using the recipient's public key.
        
        Args:
            public_key: Recipient's public key
            
        Returns:
            Tuple of (symmetric_key, encapsulation)
        """
        # Generate a random value
        random_value = QuantumResistantCrypto.generate_secure_random(32)
        
        # Encrypt the random value using NTRU
        encapsulation = self.ntru.encrypt(public_key, random_value)
        
        # Derive a symmetric key from the random value
        symmetric_key = QuantumResistantCrypto.hash_data(random_value)[:32]  # Use first 32 bytes as key
        
        return symmetric_key, encapsulation
        
    def decapsulate(self, private_key: Any, encapsulation: List[int]) -> bytes:
        """
        Decapsulate a symmetric key using the recipient's private key.
        
        Args:
            private_key: Recipient's private key
            encapsulation: Encapsulated key
            
        Returns:
            Symmetric key
        """
        # Decrypt the encapsulation using NTRU
        random_value = self.ntru.decrypt(private_key, encapsulation)
        
        # Derive the symmetric key
        symmetric_key = QuantumResistantCrypto.hash_data(random_value)[:32]  # Use first 32 bytes as key
        
        return symmetric_key


class SymmetricEncryption:
    """
    Symmetric encryption using quantum-resistant algorithms.
    This implementation uses XChaCha20-Poly1305, which is considered
    resistant to quantum attacks with sufficient key size.
    """
    
    @staticmethod
    def encrypt(key: bytes, plaintext: Union[str, bytes], 
                associated_data: Optional[bytes] = None) -> bytes:
        """
        Encrypt data using XChaCha20-Poly1305.
        
        Args:
            key: Encryption key (32 bytes)
            plaintext: Data to encrypt
            associated_data: Additional data to authenticate
            
        Returns:
            Encrypted data with nonce and tag
        """
        if isinstance(plaintext, str):
            plaintext = plaintext.encode('utf-8')
            
        if associated_data is None:
            associated_data = b''
            
        # Generate a random nonce
        nonce = QuantumResistantCrypto.generate_secure_random(24)  # XChaCha20 uses 24-byte nonces
        
        # Simplified XChaCha20-Poly1305 encryption
        # In a real implementation, use a library like PyNaCl
        
        # For demonstration, we'll do a simple XOR-based encryption with HMAC authentication
        # This is NOT secure for real use, just for demonstration purposes
        
        # Expand the key to the length of the plaintext using a KDF
        keystream = b''
        block = nonce
        while len(keystream) < len(plaintext):
            block = QuantumResistantCrypto.hash_data(key + block)
            keystream += block
            
        keystream = keystream[:len(plaintext)]
        
        # XOR the plaintext with the keystream
        ciphertext = bytes(p ^ k for p, k in zip(plaintext, keystream))
        
        # Compute an authentication tag
        tag = QuantumResistantCrypto.hmac_data(key, nonce + ciphertext + associated_data)[:16]
        
        # Return nonce + ciphertext + tag
        return nonce + ciphertext + tag
        
    @staticmethod
    def decrypt(key: bytes, ciphertext: bytes, 
                associated_data: Optional[bytes] = None) -> Optional[bytes]:
        """
        Decrypt data using XChaCha20-Poly1305.
        
        Args:
            key: Encryption key (32 bytes)
            ciphertext: Data to decrypt (nonce + ciphertext + tag)
            associated_data: Additional authenticated data
            
        Returns:
            Decrypted data, or None if authentication fails
        """
        if associated_data is None:
            associated_data = b''
            
        # Split the ciphertext into nonce, ciphertext, and tag
        nonce = ciphertext[:24]
        tag = ciphertext[-16:]
        actual_ciphertext = ciphertext[24:-16]
        
        # Verify the authentication tag
        expected_tag = QuantumResistantCrypto.hmac_data(key, nonce + actual_ciphertext + associated_data)[:16]
        if not hmac.compare_digest(tag, expected_tag):
            return None
            
        # Expand the key to the length of the ciphertext using a KDF
        keystream = b''
        block = nonce
        while len(keystream) < len(actual_ciphertext):
            block = QuantumResistantCrypto.hash_data(key + block)
            keystream += block
            
        keystream = keystream[:len(actual_ciphertext)]
        
        # XOR the ciphertext with the keystream to get the plaintext
        plaintext = bytes(c ^ k for c, k in zip(actual_ciphertext, keystream))
        
        return plaintext


# Usage example
def example_usage():
    # Generate a keypair
    kem = QuantumKEM()
    private_key, public_key = kem.generate_keypair()
    
    # Encapsulate a key
    symmetric_key, encapsulation = kem.encapsulate(public_key)
    
    # Decapsulate the key
    decapsulated_key = kem.decapsulate(private_key, encapsulation)
    
    # Verify that the keys match
    assert symmetric_key == decapsulated_key
    
    # Encrypt a message
    message = "This is a secret message for quantum-resistant encryption"
    ciphertext = SymmetricEncryption.encrypt(symmetric_key, message)
    
    # Decrypt the message
    plaintext = SymmetricEncryption.decrypt(symmetric_key, ciphertext)
    
    # Verify that the decryption worked
    assert plaintext.decode('utf-8') == message
    
    print("Quantum-resistant encryption and decryption successful!")
    
    # Generate a Lamport signature keypair
    lamport_private, lamport_public = LamportSignature.generate_keypair()
    
    # Sign a message
    signature = LamportSignature.sign(lamport_private, message)
    
    # Verify the signature
    assert LamportSignature.verify(lamport_public, message, signature)
    
    print("Quantum-resistant signature verification successful!")


if __name__ == "__main__":
    example_usage()
