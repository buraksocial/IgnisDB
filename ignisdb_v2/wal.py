"""
Write-Ahead Log (WAL) - Crash recovery mechanism

All writes are appended to WAL before being applied to MemTable.
On crash recovery, WAL is replayed to restore state.
"""

import os
import struct
import logging
import threading
from typing import Iterator, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# WAL entry types
WAL_PUT = 1
WAL_DELETE = 2

# Magic number for validation
WAL_MAGIC = b'IGWL'
WAL_VERSION = 1


class WAL:
    """
    Write-Ahead Log for crash safety.
    
    Format per entry:
    [type: 1 byte][key_len: 4 bytes][key][value_len: 4 bytes][value][checksum: 4 bytes]
    
    Checksum: CRC32 of (type + key_len + key + value_len + value)
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.wal_path = self.data_dir / "wal.log"
        self._file = None
        self._position = 0
        self._lock = threading.Lock()
    
    def open(self):
        """Open WAL file for appending."""
        self._file = open(self.wal_path, 'ab')
        self._position = self._file.tell()
        logger.debug(f"WAL opened at {self.wal_path}")
    
    def close(self):
        """Close WAL file."""
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None
    
    async def append(self, key: bytes, value: bytes, entry_type: int = WAL_PUT):
        """Append an entry to the WAL."""
        if self._file is None:
            self.open()
        
        # Build entry
        key_len = len(key)
        value_len = len(value) if value else 0
        
        # Entry format: type(1) + key_len(4) + key + value_len(4) + value
        entry_data = struct.pack('<BI', entry_type, key_len) + key
        entry_data += struct.pack('<I', value_len)
        if value:
            entry_data += value
        
        # Calculate checksum (simple sum for speed, could use crc32)
        checksum = sum(entry_data) & 0xFFFFFFFF
        
        # Write entry + checksum
        full_entry = entry_data + struct.pack('<I', checksum)
        
        # Write to file (in executor to avoid blocking)
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_sync, full_entry)
    
    def _write_sync(self, data: bytes):
        """Synchronous write helper (thread-safe)."""
        with self._lock:
            if self._file is None or self._file.closed:
                self._file = open(self.wal_path, 'ab')
            self._file.write(data)
            self._file.flush()
    
    def sync(self):
        """Force sync WAL to disk."""
        if self._file:
            self._file.flush()
            os.fsync(self._file.fileno())
    
    def replay(self) -> Iterator[Tuple[int, bytes, Optional[bytes]]]:
        """
        Replay WAL entries.
        
        Yields: (entry_type, key, value)
        """
        if not self.wal_path.exists():
            return
        
        try:
            with open(self.wal_path, 'rb') as f:
                while True:
                    # Read type
                    type_data = f.read(1)
                    if len(type_data) < 1:
                        break
                    entry_type = struct.unpack('<B', type_data)[0]
                    
                    # Read key length
                    key_len_data = f.read(4)
                    if len(key_len_data) < 4:
                        break
                    key_len = struct.unpack('<I', key_len_data)[0]
                    
                    # Sanity check
                    if key_len > 10 * 1024 * 1024:  # 10 MB max key
                        logger.warning(f"WAL corruption detected: invalid key_len {key_len}")
                        break
                    
                    # Read key
                    key = f.read(key_len)
                    if len(key) < key_len:
                        break
                    
                    # Read value length
                    value_len_data = f.read(4)
                    if len(value_len_data) < 4:
                        break
                    value_len = struct.unpack('<I', value_len_data)[0]
                    
                    # Read value
                    value = None
                    if value_len > 0:
                        value = f.read(value_len)
                        if len(value) < value_len:
                            break
                    
                    # Read and verify checksum
                    checksum_data = f.read(4)
                    if len(checksum_data) < 4:
                        break
                    stored_checksum = struct.unpack('<I', checksum_data)[0]
                    
                    # Verify checksum
                    entry_data = type_data + key_len_data + key + value_len_data
                    if value:
                        entry_data += value
                    computed_checksum = sum(entry_data) & 0xFFFFFFFF
                    
                    if stored_checksum != computed_checksum:
                        logger.warning("WAL checksum mismatch, stopping replay")
                        break
                    
                    yield entry_type, key, value
                    
        except Exception as e:
            logger.error(f"Error replaying WAL: {e}")
    
    def truncate(self):
        """Truncate WAL after successful flush to SSTable (thread-safe)."""
        with self._lock:
            if self._file:
                self._file.flush()
                self._file.close()
                self._file = None
            
            # Atomic truncate: rename to .old, create new, delete old
            old_path = self.wal_path.with_suffix('.old')
            try:
                if self.wal_path.exists():
                    self.wal_path.rename(old_path)
                
                # Create new empty WAL
                self._file = open(self.wal_path, 'wb')
                self._position = 0
                
                # Delete old WAL
                if old_path.exists():
                    old_path.unlink()
                    
                logger.debug("WAL truncated")
            except Exception as e:
                logger.error(f"Error truncating WAL: {e}")
                # Reopen original if rename failed
                if not self._file or self._file.closed:
                    self._file = open(self.wal_path, 'ab')
    
    @property
    def size(self) -> int:
        """Current WAL file size."""
        try:
            return self.wal_path.stat().st_size
        except FileNotFoundError:
            return 0
