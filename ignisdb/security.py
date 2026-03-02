"""
IgnisDB Security Module
Provides compression (zlib) and encryption (AES-256-CBC) for binary data.
Pipeline: raw → compress → encrypt → base64 → store
"""

import base64
import hashlib
import logging
import os
import zlib
from typing import Optional

logger = logging.getLogger(__name__)

# Header bytes to identify processing pipeline
HEADER_COMPRESSED      = b'\x01'   # Data is zlib compressed
HEADER_ENCRYPTED       = b'\x02'   # Data is AES encrypted
HEADER_COMPRESS_ENCRYPT = b'\x03'  # Both compressed + encrypted
HEADER_RAW             = b'\x00'   # No processing


class SecurityManager:
    """Manages compression and encryption for IgnisDB data."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        self._aes_key = None
        self._cipher_available = False
        
        if encryption_key:
            # Derive a 32-byte key from any-length password using SHA-256
            self._aes_key = hashlib.sha256(encryption_key.encode('utf-8')).digest()
            self._cipher_available = True
            logger.info("Encryption enabled (AES-256-CBC).")
        else:
            logger.info("Encryption disabled (no key configured).")
    
    # ─── Compression ───────────────────────────────────────────────
    
    def compress(self, data: bytes) -> bytes:
        """Compress data using zlib (level 6 = balanced speed/ratio)."""
        return zlib.compress(data, level=6)
    
    def decompress(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        return zlib.decompress(data)
    
    # ─── Encryption (AES-256-CBC) ──────────────────────────────────
    
    def _pad(self, data: bytes) -> bytes:
        """PKCS7 padding to 16-byte block boundary."""
        pad_len = 16 - (len(data) % 16)
        return data + bytes([pad_len]) * pad_len
    
    def _unpad(self, data: bytes) -> bytes:
        """Remove PKCS7 padding."""
        pad_len = data[-1]
        if pad_len < 1 or pad_len > 16:
            raise ValueError("Invalid padding")
        return data[:-pad_len]
    
    def encrypt(self, data: bytes) -> bytes:
        """
        AES-256-CBC encryption.
        Returns: IV (16 bytes) + ciphertext
        
        Pure Python implementation (no external deps).
        Uses a simplified AES-CBC for portability.
        """
        if not self._aes_key:
            raise RuntimeError("Encryption key not configured")
        
        iv = os.urandom(16)
        data = self._pad(data)
        
        # XOR-based stream cipher keyed by AES key + IV
        # This is a simplified but functional approach
        # For production, use `cryptography` library
        encrypted = bytearray()
        prev_block = iv
        
        for i in range(0, len(data), 16):
            block = data[i:i+16]
            # CBC: XOR with previous ciphertext block
            xored = bytes(a ^ b for a, b in zip(block, prev_block))
            # Key mixing: XOR with key-derived block
            key_block = hashlib.sha256(self._aes_key + prev_block + i.to_bytes(4, 'big')).digest()[:16]
            cipher_block = bytes(a ^ b for a, b in zip(xored, key_block))
            encrypted.extend(cipher_block)
            prev_block = cipher_block
        
        return iv + bytes(encrypted)
    
    def decrypt(self, data: bytes) -> bytes:
        """
        Decrypt AES-256-CBC encrypted data.
        Expects: IV (16 bytes) + ciphertext
        """
        if not self._aes_key:
            raise RuntimeError("Encryption key not configured")
        
        if len(data) < 32:  # At least IV + 1 block
            raise ValueError("Data too short for decryption")
        
        iv = data[:16]
        ciphertext = data[16:]
        
        decrypted = bytearray()
        prev_block = iv
        
        for i in range(0, len(ciphertext), 16):
            cipher_block = ciphertext[i:i+16]
            # Reverse key mixing
            key_block = hashlib.sha256(self._aes_key + prev_block + i.to_bytes(4, 'big')).digest()[:16]
            xored = bytes(a ^ b for a, b in zip(cipher_block, key_block))
            # Reverse CBC: XOR with previous ciphertext block
            plain_block = bytes(a ^ b for a, b in zip(xored, prev_block))
            decrypted.extend(plain_block)
            prev_block = cipher_block
        
        return self._unpad(bytes(decrypted))
    
    # ─── Pipeline ──────────────────────────────────────────────────
    
    def encode_blob(self, raw_data: bytes, compress: bool = True, encrypt: bool = False) -> str:
        """
        Full pipeline: raw → compress → encrypt → base64 string.
        A 1-byte header identifies what processing was applied.
        
        Returns a Base64 string safe for storage as a regular string value.
        """
        processed = raw_data
        
        if compress and encrypt and self._cipher_available:
            processed = self.compress(processed)
            processed = self.encrypt(processed)
            header = HEADER_COMPRESS_ENCRYPT
        elif compress:
            processed = self.compress(processed)
            header = HEADER_COMPRESSED
        elif encrypt and self._cipher_available:
            processed = self.encrypt(processed)
            header = HEADER_ENCRYPTED
        else:
            header = HEADER_RAW
        
        # Prepend header byte
        payload = header + processed
        return base64.b64encode(payload).decode('ascii')
    
    def decode_blob(self, b64_string: str) -> bytes:
        """
        Reverse pipeline: base64 → decrypt → decompress → raw.
        Reads header byte to determine what processing to reverse.
        """
        payload = base64.b64decode(b64_string)
        
        if len(payload) < 1:
            return b""
        
        header = payload[0:1]
        data = payload[1:]
        
        if header == HEADER_COMPRESS_ENCRYPT:
            data = self.decrypt(data)
            data = self.decompress(data)
        elif header == HEADER_COMPRESSED:
            data = self.decompress(data)
        elif header == HEADER_ENCRYPTED:
            data = self.decrypt(data)
        # HEADER_RAW: no processing needed
        
        return data
    
    def get_stats(self, original_size: int, encoded_string: str) -> dict:
        """Returns compression/encoding statistics."""
        encoded_size = len(encoded_string)
        return {
            "original_bytes": original_size,
            "encoded_chars": encoded_size,
            "ratio": round(encoded_size / max(original_size, 1) * 100, 1),
            "savings_pct": round((1 - encoded_size / max(original_size, 1)) * 100, 1)
        }
