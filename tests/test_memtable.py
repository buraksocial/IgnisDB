"""Tests for SkipList MemTable"""
import pytest
from ignisdb.memtable import SkipList


class TestSkipList:
    def test_put_get(self):
        sl = SkipList()
        sl.put(b'key1', b'value1')
        sl.put(b'key2', b'value2')
        
        assert sl.get(b'key1') == b'value1'
        assert sl.get(b'key2') == b'value2'
        assert sl.get(b'missing') is None
    
    def test_update(self):
        sl = SkipList()
        sl.put(b'key', b'old')
        sl.put(b'key', b'new')
        
        assert sl.get(b'key') == b'new'
        assert len(sl) == 1
    
    def test_delete(self):
        sl = SkipList()
        sl.put(b'key1', b'value1')
        sl.put(b'key2', b'value2')
        
        assert sl.delete(b'key1')
        assert sl.get(b'key1') is None
        assert sl.get(b'key2') == b'value2'
        assert len(sl) == 1
    
    def test_sorted_iteration(self):
        sl = SkipList()
        keys = [b'charlie', b'alpha', b'bravo', b'delta']
        for k in keys:
            sl.put(k, k + b'_value')
        
        result = list(sl.keys())
        assert result == sorted(keys)
    
    def test_range_iter(self):
        sl = SkipList()
        for i in range(10):
            sl.put(f'key:{i:02d}'.encode(), f'val:{i}'.encode())
        
        result = list(sl.range_iter(b'key:03', b'key:07'))
        expected_keys = [f'key:{i:02d}'.encode() for i in range(3, 8)]
        assert [k for k, v in result] == expected_keys
    
    def test_contains(self):
        sl = SkipList()
        sl.put(b'exists', b'yes')
        
        assert b'exists' in sl
        assert b'missing' not in sl
    
    def test_size_tracking(self):
        sl = SkipList()
        assert sl.size == 0
        
        sl.put(b'key1', b'value1')
        assert sl.size == len(b'key1') + len(b'value1')
        
        sl.put(b'key2', b'value2')
        assert sl.size == len(b'key1') + len(b'value1') + len(b'key2') + len(b'value2')
    
    def test_large_dataset(self):
        sl = SkipList()
        n = 10000
        
        for i in range(n):
            sl.put(f'key:{i:08d}'.encode(), f'value:{i}'.encode())
        
        assert len(sl) == n
        
        # Verify all keys retrievable
        for i in range(n):
            assert sl.get(f'key:{i:08d}'.encode()) == f'value:{i}'.encode()
    
    def test_clear(self):
        sl = SkipList()
        for i in range(100):
            sl.put(f'key:{i}'.encode(), b'value')
        
        sl.clear()
        assert len(sl) == 0
        assert sl.size == 0
