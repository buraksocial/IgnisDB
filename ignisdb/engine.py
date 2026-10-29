"""
LSMEngine - Log-Structured Merge Tree Storage Engine

Core storage layer combining:
- MemTable (SkipList) for writes
- WAL for crash safety
- SSTables for persistent sorted storage
- TieredCache for hot data
- Bloom filters for read optimization
- Background compaction
"""

import os
import asyncio
import logging
import time
from typing import Optional, Dict, Any, List
from pathlib import Path

from .memtable import SkipList
from .wal import WAL, WAL_PUT, WAL_DELETE
from .sstable import SSTable
from .cache import TieredCache
from .sorted_set import SortedSet
from .search import InvertedIndex

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_MEMTABLE_SIZE = 16 * 1024 * 1024   # 16 MB
DEFAULT_CACHE_SIZE = 64 * 1024 * 1024      # 64 MB
DEFAULT_COMPACTION_THRESHOLD = 4           # L0 SSTables before compaction


class LSMEngine:
    """
    Log-Structured Merge Tree storage engine.
    
    Write path:
        1. Append to WAL (crash safety)
        2. Insert into MemTable (SkipList)
        3. When MemTable full → flush to SSTable
        4. Background compaction merges SSTables
    
    Read path:
        1. Check Hot Cache (LRU)
        2. Check MemTable
        3. Check Immutable MemTables
        4. Check SSTables (newest first, bloom filter optimized)
    """
    
    def __init__(self, data_dir: str, 
                 memtable_size: int = DEFAULT_MEMTABLE_SIZE,
                 cache_size: int = DEFAULT_CACHE_SIZE):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.memtable_size = memtable_size
        
        # Core components
        self.memtable = SkipList()
        self.immutables: List[SkipList] = []
        self.sstables: List[SSTable] = []
        self.wal = WAL(str(self.data_dir))
        self.cache = TieredCache(cache_size)
        
        # Secondary indexes (kept in memory for now, persisted via snapshots)
        self.sorted_sets: Dict[bytes, SortedSet] = {}
        self.search_index = InvertedIndex()
        
        # Metadata store for complex types (lists, hashes, sets)
        # Key format: __meta__:<type>:<key>
        # This allows storing structured data in the KV engine
        
        # Compaction state
        self._compacting = False
        self._flush_lock = asyncio.Lock()
        self._sequence = 0
        
        # Statistics
        self.stats = {
            'writes': 0,
            'reads': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'sstable_reads': 0,
            'compactions': 0,
        }
    
    async def open(self):
        """Initialize engine: replay WAL, load SSTables."""
        logger.info(f"Opening LSMEngine at {self.data_dir}")
        
        # Load existing SSTables
        await self._load_sstables()
        
        # Open WAL and replay
        self.wal.open()
        await self._replay_wal()
        
        logger.info(f"LSMEngine ready: {len(self.sstables)} SSTables, "
                    f"{len(self.memtable)} entries in MemTable")
    
    async def close(self):
        """Shutdown engine: flush MemTable, close files."""
        logger.info("Closing LSMEngine...")
        
        # Flush MemTable if not empty
        if len(self.memtable) > 0:
            await self.flush_memtable()
        
        # Close SSTables
        for sst in self.sstables:
            sst.close()
        
        # Close WAL
        self.wal.close()
        
        logger.info("LSMEngine closed")
    
    async def put(self, key: bytes, value: bytes):
        """Write a key-value pair."""
        # 1. Write to WAL
        await self.wal.append(key, value, WAL_PUT)
        
        # 2. Write to MemTable
        self.memtable.put(key, value)
        self.stats['writes'] += 1
        
        # 3. Update cache
        self.cache.put(key, value)
        
        # 4. Check if MemTable needs flush
        if self.memtable.size >= self.memtable_size:
            asyncio.create_task(self.flush_memtable())
    
    async def get(self, key: bytes) -> Optional[bytes]:
        """Read a value by key."""
        self.stats['reads'] += 1
        
        # 1. Check hot cache
        cached = self.cache.get(key)
        if cached is not None:
            self.stats['cache_hits'] += 1
            # Empty value = tombstone (deleted)
            return cached if cached != b'' else None
        
        self.stats['cache_misses'] += 1
        
        # 2. Check MemTable
        value = self.memtable.get(key)
        if value is not None:
            # Empty value = tombstone
            if value == b'':
                return None
            self.cache.put(key, value)
            return value
        
        # 3. Check immutable MemTables (newest first)
        for imm in reversed(self.immutables):
            value = imm.get(key)
            if value is not None:
                if value == b'':
                    return None
                self.cache.put(key, value)
                return value
        
        # 4. Check SSTables (newest first)
        for sst in reversed(self.sstables):
            if sst.might_contain(key):
                value = sst.get(key)
                if value is not None:
                    self.stats['sstable_reads'] += 1
                    # Empty value = tombstone
                    if value == b'':
                        return None
                    self.cache.put(key, value)
                    return value
        
        return None
    
    async def delete(self, key: bytes) -> bool:
        """Delete a key (tombstone)."""
        # Write tombstone to WAL
        await self.wal.append(key, b'', WAL_DELETE)
        
        # Write tombstone to MemTable (empty value = deleted)
        self.memtable.put(key, b'')
        
        # Remove from cache
        self.cache.delete(key)
        
        return True
    
    async def flush_memtable(self):
        """Flush current MemTable to SSTable on disk."""
        async with self._flush_lock:
            if len(self.memtable) == 0:
                return
            
            # Swap MemTable
            old_memtable = self.memtable
            self.memtable = SkipList()
            self.immutables.append(old_memtable)
            
            # Create SSTable
            self._sequence += 1
            try:
                sstable = await SSTable.create(
                    old_memtable, 
                    str(self.data_dir),
                    self._sequence
                )
                self.sstables.append(sstable)
                
                # Truncate WAL after successful flush
                self.wal.truncate()
                
                # Remove from immutables
                self.immutables.remove(old_memtable)
                
                logger.info(f"Flushed MemTable to SSTable #{self._sequence} "
                           f"({len(old_memtable)} entries)")
                
                # Trigger compaction if needed
                if len(self.sstables) >= DEFAULT_COMPACTION_THRESHOLD:
                    asyncio.create_task(self.compact())
                    
            except Exception as e:
                logger.error(f"Failed to flush MemTable: {e}")
                # Restore MemTable on failure
                self.memtable = old_memtable
                self.immutables.pop()
    
    async def compact(self):
        """
        Background compaction: merge multiple SSTables into one.
        
        Simple strategy: merge all SSTables when threshold exceeded.
        Production would use leveled or tiered compaction.
        """
        if self._compacting or len(self.sstables) < 2:
            return
        
        self._compacting = True
        try:
            logger.info(f"Starting compaction ({len(self.sstables)} SSTables)")
            
            # Merge all SSTables into a new SkipList
            merged = SkipList()
            for sst in self.sstables:
                for key, value in sst.iter_all():
                    # Skip tombstones during compaction
                    if value:
                        merged.put(key, value)
            
            if len(merged) == 0:
                self._compacting = False
                return
            
            # Create new compacted SSTable
            self._sequence += 1
            new_sst = await SSTable.create(merged, str(self.data_dir), self._sequence)
            
            # Close old SSTables
            old_sstables = self.sstables.copy()
            for sst in old_sstables:
                sst.close()
                # Optionally delete old files
                try:
                    os.unlink(sst.path)
                except OSError:
                    pass
            
            # Replace with compacted SSTable
            self.sstables = [new_sst]
            self.stats['compactions'] += 1
            
            logger.info(f"Compaction complete: {len(old_sstables)} → 1 SSTable "
                       f"({len(merged)} entries)")
            
        except Exception as e:
            logger.error(f"Compaction failed: {e}")
        finally:
            self._compacting = False
    
    async def _load_sstables(self):
        """Load existing SSTables from disk."""
        sst_files = sorted(self.data_dir.glob("sstable_*.db"))
        
        for path in sst_files:
            try:
                sst = SSTable(str(path))
                sst.open()
                self.sstables.append(sst)
                # Extract sequence number from filename
                try:
                    seq = int(path.stem.split('_')[1])
                    self._sequence = max(self._sequence, seq)
                except (IndexError, ValueError):
                    pass
            except Exception as e:
                logger.error(f"Failed to load SSTable {path}: {e}")
        
        logger.info(f"Loaded {len(self.sstables)} SSTables")
    
    async def _replay_wal(self):
        """Replay WAL entries into MemTable."""
        count = 0
        for entry_type, key, value in self.wal.replay():
            if entry_type == WAL_PUT:
                self.memtable.put(key, value)
                count += 1
            elif entry_type == WAL_DELETE:
                self.memtable.put(key, b'')  # Tombstone
                count += 1
        
        if count > 0:
            logger.info(f"Replayed {count} entries from WAL")
    
    # === Sorted Set Operations ===
    
    def zadd(self, key: bytes, score: float, member: bytes) -> int:
        """Add member to sorted set."""
        if key not in self.sorted_sets:
            self.sorted_sets[key] = SortedSet()
        return self.sorted_sets[key].zadd(score, member)
    
    def zrem(self, key: bytes, member: bytes) -> int:
        """Remove member from sorted set."""
        if key not in self.sorted_sets:
            return 0
        return self.sorted_sets[key].zrem(member)
    
    def zscore(self, key: bytes, member: bytes) -> Optional[float]:
        """Get member score."""
        if key not in self.sorted_sets:
            return None
        return self.sorted_sets[key].zscore(member)
    
    def zrange(self, key: bytes, start: int, stop: int) -> List[bytes]:
        """Range query by rank."""
        if key not in self.sorted_sets:
            return []
        return self.sorted_sets[key].zrange(start, stop)
    
    def zrevrange(self, key: bytes, start: int, stop: int) -> List[bytes]:
        """Reverse range query by rank."""
        if key not in self.sorted_sets:
            return []
        return self.sorted_sets[key].zrevrange(start, stop)
    
    def zrangebyscore(self, key: bytes, min_score: float, max_score: float,
                      offset: int = 0, limit: int = -1) -> List[bytes]:
        """Range query by score."""
        if key not in self.sorted_sets:
            return []
        return self.sorted_sets[key].zrangebyscore(min_score, max_score, offset, limit)
    
    def zrevrangebyscore(self, key: bytes, max_score: float, min_score: float,
                         offset: int = 0, limit: int = -1) -> List[bytes]:
        """Reverse range query by score."""
        if key not in self.sorted_sets:
            return []
        return self.sorted_sets[key].zrevrangebyscore(max_score, min_score, offset, limit)
    
    def zcard(self, key: bytes) -> int:
        """Get sorted set cardinality."""
        if key not in self.sorted_sets:
            return 0
        return self.sorted_sets[key].zcard()
    
    # === Full-Text Search Operations ===
    
    def ft_add(self, doc_id: str, text: str):
        """Add document to search index."""
        self.search_index.add_document(doc_id, text)
    
    def ft_search(self, query: str, limit: int = 20) -> List[tuple]:
        """Search documents."""
        return self.search_index.search(query, limit)
    
    def ft_remove(self, doc_id: str):
        """Remove document from search index."""
        self.search_index.remove_document(doc_id)
    
    # === Stats & Diagnostics ===
    
    def get_stats(self) -> dict:
        """Return comprehensive engine statistics."""
        cache_stats = self.cache.stats()
        return {
            **self.stats,
            'memtable_entries': len(self.memtable),
            'memtable_bytes': self.memtable.size,
            'sstable_count': len(self.sstables),
            'sstable_total_bytes': sum(sst.size for sst in self.sstables),
            'wal_size_bytes': self.wal.size,
            'sorted_set_count': len(self.sorted_sets),
            'search_doc_count': self.search_index.doc_count,
            'cache': cache_stats,
        }


class DB:
    """
    High-level embedded database interface.
    
    Usage:
        db = DB.open('/var/lib/ignisdb')
        await db.set(b'key', b'value')
        value = await db.get(b'key')
        await db.close()
    """
    
    def __init__(self, engine: LSMEngine):
        self.engine = engine
    
    @classmethod
    async def open(cls, data_dir: str, **kwargs) -> 'DB':
        """Open or create a database."""
        engine = LSMEngine(data_dir, **kwargs)
        await engine.open()
        return cls(engine)
    
    async def close(self):
        """Close the database."""
        await self.engine.close()
    
    async def set(self, key: bytes, value: bytes):
        """Set a key-value pair."""
        if isinstance(key, str):
            key = key.encode('utf-8')
        if isinstance(value, str):
            value = value.encode('utf-8')
        await self.engine.put(key, value)
    
    async def get(self, key: bytes) -> Optional[bytes]:
        """Get value by key."""
        if isinstance(key, str):
            key = key.encode('utf-8')
        return await self.engine.get(key)
    
    async def delete(self, key: bytes) -> bool:
        """Delete a key."""
        if isinstance(key, str):
            key = key.encode('utf-8')
        return await self.engine.delete(key)
    
    def stats(self) -> dict:
        """Get database statistics."""
        return self.engine.get_stats()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
