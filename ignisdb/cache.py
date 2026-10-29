"""
Tiered Cache - LRU Hot Cache with adaptive eviction

Fixed RAM budget (default 64 MB) for frequently accessed keys.
Warm layer handled by OS page cache via mmap'd SSTables.
"""

import time
import threading
from collections import OrderedDict
from typing import Optional, Dict, Tuple


class TieredCache:
    """
    Fixed-size LRU cache for hot data.
    
    - Hot layer: LRU cache with configurable max size
    - Access tracking for adaptive eviction
    - Thread-safe operations
    """
    
    def __init__(self, max_hot_bytes: int = 64 * 1024 * 1024):
        self.max_hot_bytes = max_hot_bytes
        self.hot_cache: OrderedDict[bytes, bytes] = OrderedDict()
        self.current_size = 0
        self.access_stats: Dict[bytes, Tuple[int, float]] = {}  # key -> (count, last_access)
        self._lock = threading.Lock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Get value from hot cache. Updates LRU order."""
        with self._lock:
            if key in self.hot_cache:
                # Move to end (most recently used)
                self.hot_cache.move_to_end(key)
                self._update_access_stats(key)
                self.hits += 1
                return self.hot_cache[key]
            self.misses += 1
            return None
    
    def put(self, key: bytes, value: bytes):
        """Put value into hot cache with LRU eviction."""
        size = len(key) + len(value)
        
        # Don't cache items larger than 10% of cache
        if size > self.max_hot_bytes // 10:
            return
        
        with self._lock:
            # Update existing key
            if key in self.hot_cache:
                old_value = self.hot_cache[key]
                self.current_size -= len(key) + len(old_value)
                self.hot_cache.move_to_end(key)
            
            # Evict until we have space
            while self.current_size + size > self.max_hot_bytes and self.hot_cache:
                self._evict_coldest()
            
            self.hot_cache[key] = value
            self.current_size += size
            self._update_access_stats(key)
    
    def delete(self, key: bytes) -> bool:
        """Remove key from cache."""
        with self._lock:
            if key in self.hot_cache:
                value = self.hot_cache.pop(key)
                self.current_size -= len(key) + len(value)
                self.access_stats.pop(key, None)
                return True
            return False
    
    def _evict_coldest(self):
        """Evict the least recently used item (first in OrderedDict)."""
        if not self.hot_cache:
            return
        
        # Simple LRU: pop first item (least recently used)
        key, value = self.hot_cache.popitem(last=False)
        self.current_size -= len(key) + len(value)
        self.access_stats.pop(key, None)
    
    def _update_access_stats(self, key: bytes):
        """Update access statistics for a key."""
        if key in self.access_stats:
            count, _ = self.access_stats[key]
            self.access_stats[key] = (count + 1, time.time())
        else:
            self.access_stats[key] = (1, time.time())
    
    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self.hot_cache.clear()
            self.access_stats.clear()
            self.current_size = 0
    
    def stats(self) -> dict:
        """Return cache statistics."""
        with self._lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
            return {
                'hot_cache_entries': len(self.hot_cache),
                'hot_cache_bytes': self.current_size,
                'max_hot_bytes': self.max_hot_bytes,
                'utilization_pct': (self.current_size / self.max_hot_bytes * 100) if self.max_hot_bytes > 0 else 0,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate_pct': round(hit_rate, 2),
            }
    
    def __contains__(self, key: bytes) -> bool:
        with self._lock:
            return key in self.hot_cache
    
    def __len__(self) -> int:
        return len(self.hot_cache)
