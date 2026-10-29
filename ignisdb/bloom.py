"""
Bloom Filter - Probabilistic set membership test

False positive rate: ~1% (10 bits per key, 7 hash functions)
Memory: 10K keys → ~12 KB
"""

import math
import struct
import hashlib
from typing import Iterable


class BloomFilter:
    """
    Space-efficient probabilistic data structure for set membership testing.
    
    Uses multiple hash functions to map elements to bit positions.
    False positives are possible, but false negatives are not.
    """
    
    def __init__(self, expected_items: int = 10000, fp_rate: float = 0.01):
        # Optimal size calculation: m = -(n * ln(p)) / (ln(2)^2)
        self.size = max(64, int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2)))
        # Optimal hash count: k = (m/n) * ln(2)
        self.hash_count = max(1, int((self.size / max(1, expected_items)) * math.log(2)))
        self.bits = bytearray(self.size // 8 + 1)
        self._count = 0
    
    @classmethod
    def from_keys(cls, keys: Iterable[bytes], expected_items: int = None, fp_rate: float = 0.01) -> 'BloomFilter':
        """Create a bloom filter from an iterable of keys."""
        keys_list = list(keys)
        if expected_items is None:
            expected_items = len(keys_list)
        bf = cls(expected_items, fp_rate)
        for key in keys_list:
            bf.add(key)
        return bf
    
    def _hash(self, key: bytes, seed: int) -> int:
        """Generate a hash value using SHA-256 with different seeds."""
        h = hashlib.sha256(struct.pack('<I', seed) + key).digest()
        return int.from_bytes(h[:8], 'little') % self.size
    
    def add(self, key: bytes):
        """Add a key to the bloom filter."""
        for i in range(self.hash_count):
            pos = self._hash(key, i)
            self.bits[pos // 8] |= (1 << (pos % 8))
        self._count += 1
    
    def might_contain(self, key: bytes) -> bool:
        """Check if a key might be in the set. False positives possible."""
        for i in range(self.hash_count):
            pos = self._hash(key, i)
            if not (self.bits[pos // 8] & (1 << (pos % 8))):
                return False
        return True
    
    def serialize(self) -> bytes:
        """Serialize the bloom filter to bytes."""
        header = struct.pack('<III', self.size, self.hash_count, self._count)
        return header + bytes(self.bits)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'BloomFilter':
        """Deserialize a bloom filter from bytes."""
        size, hash_count, count = struct.unpack('<III', data[:12])
        bf = cls.__new__(cls)
        bf.size = size
        bf.hash_count = hash_count
        bf._count = count
        bf.bits = bytearray(data[12:])
        return bf
    
    @property
    def memory_usage(self) -> int:
        """Return memory usage in bytes."""
        return len(self.bits) + 12  # bits + header overhead
