"""Tests for TieredCache"""
import pytest
from ignisdb.cache import TieredCache


class TestTieredCache:
    def test_basic_put_get(self):
        cache = TieredCache(max_hot_bytes=1024)
        cache.put(b'key', b'value')
        
        assert cache.get(b'key') == b'value'
    
    def test_cache_miss(self):
        cache = TieredCache(max_hot_bytes=1024)
        assert cache.get(b'missing') is None
    
    def test_lru_eviction(self):
        cache = TieredCache(max_hot_bytes=1024)
        
        # Fill cache with items that fit
        for i in range(8):
            cache.put(f'key{i}'.encode(), b'x' * 10)
        
        # Access some keys to make them hot (move to end of OrderedDict)
        cache.get(b'key6')
        cache.get(b'key7')
        
        # Add more to trigger eviction of cold items (key0, key1, ...)
        for i in range(8, 20):
            cache.put(f'key{i}'.encode(), b'x' * 10)
        
        # Recently accessed should still be there
        assert cache.get(b'key7') is not None
        assert cache.get(b'key6') is not None
    
    def test_update_existing(self):
        cache = TieredCache(max_hot_bytes=1024)
        cache.put(b'key', b'old')
        cache.put(b'key', b'new_value')
        
        assert cache.get(b'key') == b'new_value'
    
    def test_delete(self):
        cache = TieredCache(max_hot_bytes=1024)
        cache.put(b'key', b'value')
        
        assert cache.delete(b'key')
        assert cache.get(b'key') is None
        assert not cache.delete(b'key')  # Already deleted
    
    def test_stats(self):
        cache = TieredCache(max_hot_bytes=1024)
        cache.put(b'key', b'value')
        cache.get(b'key')      # hit
        cache.get(b'missing')  # miss
        
        stats = cache.stats()
        assert stats['hits'] == 1
        assert stats['misses'] == 1
        assert stats['hot_cache_entries'] == 1
    
    def test_large_item_not_cached(self):
        cache = TieredCache(max_hot_bytes=100)
        large_value = b'x' * 50  # > 10% of cache
        
        cache.put(b'key', large_value)
        assert cache.get(b'key') is None  # Not cached
    
    def test_clear(self):
        cache = TieredCache(max_hot_bytes=1024)
        for i in range(10):
            cache.put(f'key{i}'.encode(), b'value')
        
        cache.clear()
        assert len(cache) == 0
        assert cache.current_size == 0
    
    def test_contains(self):
        cache = TieredCache(max_hot_bytes=1024)
        cache.put(b'key', b'value')
        
        assert b'key' in cache
        assert b'missing' not in cache
