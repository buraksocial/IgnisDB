"""
SSTable - Sorted String Table on disk with mmap access

Format:
[Header: 16 bytes]
  - magic: 4 bytes ("IGDB")
  - version: 2 bytes
  - entry_count: 4 bytes
  - reserved: 6 bytes

[Data Block: variable]
  - key_len (4 bytes) + key + value_len (4 bytes) + value
  - ... (sorted by key)

[Index Block: variable]
  - sparse index: first key of each ~4KB block + offset (8 bytes)

[Bloom Filter: variable]
  - serialized BloomFilter

[Footer: 16 bytes]
  - index_offset: 8 bytes
  - bloom_offset: 8 bytes
"""

import os
import mmap
import struct
import time
import logging
from typing import Optional, Iterator, Tuple, List
from pathlib import Path

from .bloom import BloomFilter
from .memtable import SkipList

logger = logging.getLogger(__name__)

# Constants
MAGIC = b'IGDB'
VERSION = 1
BLOCK_SIZE = 4096  # Sparse index granularity


class SSTable:
    """
    Immutable sorted string table stored on disk.
    
    Uses memory-mapped I/O for efficient reads.
    Includes sparse index and bloom filter for fast lookups.
    """
    
    def __init__(self, path: str):
        self.path = Path(path)
        self._file = None
        self._mm: Optional[mmap.mmap] = None
        self._entry_count = 0
        self._index_offset = 0
        self._bloom_offset = 0
        self._bloom: Optional[BloomFilter] = None
        self._index: List[Tuple[bytes, int]] = []  # [(first_key, offset), ...]
        self._loaded = False
    
    def open(self):
        """Open SSTable file and load metadata."""
        if self._loaded:
            return
        
        self._file = open(self.path, 'rb')
        file_size = os.path.getsize(self.path)
        
        if file_size == 0:
            raise ValueError(f"Empty SSTable file: {self.path}")
        
        self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
        
        # Read footer (last 16 bytes)
        self._mm.seek(-16, 2)
        footer_data = self._mm.read(16)
        self._index_offset, self._bloom_offset = struct.unpack('<QQ', footer_data)
        
        # Read header
        self._mm.seek(0)
        magic = self._mm.read(4)
        if magic != MAGIC:
            raise ValueError(f"Invalid SSTable magic: {magic}")
        
        version, self._entry_count = struct.unpack('<HI', self._mm.read(6))
        self._mm.read(6)  # reserved
        
        # Load bloom filter
        if self._bloom_offset > 0 and self._bloom_offset < file_size:
            bloom_size = self._index_offset - self._bloom_offset if self._index_offset > self._bloom_offset else file_size - 16 - self._bloom_offset
            if bloom_size > 0:
                self._mm.seek(self._bloom_offset)
                bloom_data = self._mm.read(bloom_size)
                try:
                    self._bloom = BloomFilter.deserialize(bloom_data)
                except Exception as e:
                    logger.warning(f"Failed to load bloom filter: {e}")
        
        # Load sparse index
        if self._index_offset > 0 and self._index_offset < self._bloom_offset:
            self._mm.seek(self._index_offset)
            index_data_size = self._bloom_offset - self._index_offset
            index_data = self._mm.read(index_data_size)
            self._parse_index(index_data)
        
        self._loaded = True
        logger.debug(f"SSTable opened: {self.path} ({self._entry_count} entries)")
    
    def _parse_index(self, data: bytes):
        """Parse sparse index from bytes."""
        self._index = []
        pos = 0
        while pos < len(data):
            # Each entry: key_len(4) + key + offset(8)
            if pos + 4 > len(data):
                break
            key_len = struct.unpack('<I', data[pos:pos+4])[0]
            pos += 4
            
            if pos + key_len + 8 > len(data):
                break
            key = data[pos:pos+key_len]
            pos += key_len
            
            offset = struct.unpack('<Q', data[pos:pos+8])[0]
            pos += 8
            
            self._index.append((key, offset))
    
    def close(self):
        """Close SSTable file."""
        if self._mm:
            self._mm.close()
            self._mm = None
        if self._file:
            self._file.close()
            self._file = None
        self._loaded = False
    
    def might_contain(self, key: bytes) -> bool:
        """Check bloom filter for possible key presence."""
        if not self._loaded:
            self.open()
        if self._bloom is None:
            return True  # No bloom filter, must check
        return self._bloom.might_contain(key)
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        Look up a key in the SSTable.
        
        Uses bloom filter → sparse index → linear scan within block.
        """
        if not self._loaded:
            self.open()
        
        # Bloom filter check
        if not self.might_contain(key):
            return None
        
        # Find approximate position using sparse index
        start_offset = self._find_block_offset(key)
        
        # Linear scan within block
        self._mm.seek(start_offset)
        
        while self._mm.tell() < self._index_offset:
            # Read key length
            key_len_data = self._mm.read(4)
            if len(key_len_data) < 4:
                break
            key_len = struct.unpack('<I', key_len_data)[0]
            
            if key_len == 0 or key_len > 10 * 1024 * 1024:
                break
            
            # Read key
            current_key = self._mm.read(key_len)
            if len(current_key) < key_len:
                break
            
            # Read value length
            value_len_data = self._mm.read(4)
            if len(value_len_data) < 4:
                break
            value_len = struct.unpack('<I', value_len_data)[0]
            
            # Check if this is our key
            if current_key == key:
                value = self._mm.read(value_len)
                return value if len(value) == value_len else None
            elif current_key > key:
                # Past our key (data is sorted)
                break
            
            # Skip value
            self._mm.seek(value_len, 1)
        
        return None
    
    def _find_block_offset(self, key: bytes) -> int:
        """Find the starting offset for the block that might contain key."""
        if not self._index:
            return 16  # After header
        
        # Binary search in sparse index
        lo, hi = 0, len(self._index) - 1
        result_offset = 16  # Default to start of data
        
        while lo <= hi:
            mid = (lo + hi) // 2
            idx_key, idx_offset = self._index[mid]
            
            if idx_key <= key:
                result_offset = idx_offset
                lo = mid + 1
            else:
                hi = mid - 1
        
        return result_offset
    
    def iter_all(self) -> Iterator[Tuple[bytes, bytes]]:
        """Iterate over all key-value pairs in sorted order."""
        if not self._loaded:
            self.open()
        
        self._mm.seek(16)  # After header
        
        for _ in range(self._entry_count):
            # Read key length
            key_len_data = self._mm.read(4)
            if len(key_len_data) < 4:
                break
            key_len = struct.unpack('<I', key_len_data)[0]
            
            # Read key
            key = self._mm.read(key_len)
            if len(key) < key_len:
                break
            
            # Read value length
            value_len_data = self._mm.read(4)
            if len(value_len_data) < 4:
                break
            value_len = struct.unpack('<I', value_len_data)[0]
            
            # Read value
            value = self._mm.read(value_len)
            if len(value) < value_len:
                break
            
            yield key, value
    
    @property
    def entry_count(self) -> int:
        return self._entry_count
    
    @property
    def size(self) -> int:
        """File size in bytes."""
        try:
            return self.path.stat().st_size
        except FileNotFoundError:
            return 0
    
    @classmethod
    async def create(cls, memtable: SkipList, data_dir: str, sequence: int = None) -> 'SSTable':
        """
        Create an SSTable from a MemTable.
        
        Writes sorted key-value pairs to disk with index and bloom filter.
        """
        if sequence is None:
            sequence = int(time.time() * 1000)
        
        path = Path(data_dir) / f"sstable_{sequence:016d}.db"
        entries = list(memtable.items())
        
        if not entries:
            raise ValueError("Cannot create SSTable from empty MemTable")
        
        # Build bloom filter
        bloom = BloomFilter.from_keys([k for k, v in entries], len(entries))
        
        # Write SSTable synchronously (in executor)
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, cls._write_sstable, path, entries, bloom)
        
        # Open and return
        sstable = cls(str(path))
        sstable.open()
        return sstable
    
    @staticmethod
    def _write_sstable(path: Path, entries: List[Tuple[bytes, bytes]], bloom: BloomFilter):
        """Write SSTable file (runs in executor)."""
        with open(path, 'wb') as f:
            # === Header (16 bytes) ===
            f.write(MAGIC)                          # 4 bytes
            f.write(struct.pack('<H', VERSION))     # 2 bytes
            f.write(struct.pack('<I', len(entries))) # 4 bytes
            f.write(b'\x00' * 6)                    # 6 bytes reserved
            
            # === Data Block ===
            data_start = f.tell()
            index_entries = []
            bytes_since_last_index = 0
            
            for i, (key, value) in enumerate(entries):
                # Record index entry at block boundaries
                if bytes_since_last_index >= BLOCK_SIZE or i == 0:
                    index_entries.append((key, f.tell()))
                    bytes_since_last_index = 0
                
                # Write entry
                f.write(struct.pack('<I', len(key)))
                f.write(key)
                f.write(struct.pack('<I', len(value)))
                f.write(value)
                
                bytes_since_last_index += 4 + len(key) + 4 + len(value)
            
            # === Index Block ===
            index_offset = f.tell()
            for idx_key, idx_offset in index_entries:
                f.write(struct.pack('<I', len(idx_key)))
                f.write(idx_key)
                f.write(struct.pack('<Q', idx_offset))
            
            # === Bloom Filter ===
            bloom_offset = f.tell()
            bloom_data = bloom.serialize()
            f.write(bloom_data)
            
            # === Footer (16 bytes) ===
            f.write(struct.pack('<QQ', index_offset, bloom_offset))
        
        logger.info(f"SSTable created: {path} ({len(entries)} entries, {path.stat().st_size} bytes)")
