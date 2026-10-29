"""Tests for BloomFilter"""
import pytest
from ignisdb_v2.bloom import BloomFilter


class TestBloomFilter:
    def test_basic_membership(self):
        bf = BloomFilter(expected_items=1000, fp_rate=0.01)
        bf.add(b'hello')
        bf.add(b'world')
        
        assert bf.might_contain(b'hello')
        assert bf.might_contain(b'world')
        assert not bf.might_contain(b'missing')
    
    def test_from_keys(self):
        keys = [f'key:{i}'.encode() for i in range(100)]
        bf = BloomFilter.from_keys(keys)
        
        for key in keys:
            assert bf.might_contain(key)
        
        assert not bf.might_contain(b'nonexistent')
    
    def test_false_positive_rate(self):
        n = 10000
        bf = BloomFilter(expected_items=n, fp_rate=0.01)
        
        for i in range(n):
            bf.add(f'key:{i}'.encode())
        
        # Test false positives
        false_positives = 0
        test_count = 10000
        for i in range(n, n + test_count):
            if bf.might_contain(f'key:{i}'.encode()):
                false_positives += 1
        
        fp_rate = false_positives / test_count
        # Allow up to 3% (generous bound for randomness)
        assert fp_rate < 0.03, f"FP rate {fp_rate:.4f} exceeds threshold"
    
    def test_serialization(self):
        bf = BloomFilter(expected_items=100)
        for i in range(50):
            bf.add(f'key:{i}'.encode())
        
        data = bf.serialize()
        bf2 = BloomFilter.deserialize(data)
        
        for i in range(50):
            assert bf2.might_contain(f'key:{i}'.encode())
        
        assert not bf2.might_contain(b'missing')
    
    def test_memory_usage(self):
        bf = BloomFilter(expected_items=10000, fp_rate=0.01)
        # Should be roughly 10 bits per item = ~12.5 KB
        assert bf.memory_usage < 20000  # Under 20 KB
