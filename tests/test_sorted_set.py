"""Tests for SortedSet"""
import pytest
from ignisdb_v2.sorted_set import SortedSet


class TestSortedSet:
    def test_zadd_zscore(self):
        ss = SortedSet()
        assert ss.zadd(1.0, b'member1') == 1  # New
        assert ss.zadd(2.0, b'member2') == 1
        assert ss.zadd(1.5, b'member1') == 0  # Update
        
        assert ss.zscore(b'member1') == 1.5
        assert ss.zscore(b'member2') == 2.0
        assert ss.zscore(b'missing') is None
    
    def test_zcard(self):
        ss = SortedSet()
        assert ss.zcard() == 0
        
        ss.zadd(1.0, b'a')
        ss.zadd(2.0, b'b')
        assert ss.zcard() == 2
    
    def test_zrem(self):
        ss = SortedSet()
        ss.zadd(1.0, b'member')
        
        assert ss.zrem(b'member') == 1
        assert ss.zrem(b'member') == 0  # Already removed
        assert ss.zcard() == 0
    
    def test_zrange(self):
        ss = SortedSet()
        ss.zadd(3.0, b'c')
        ss.zadd(1.0, b'a')
        ss.zadd(2.0, b'b')
        
        assert ss.zrange(0, -1) == [b'a', b'b', b'c']
        assert ss.zrange(0, 1) == [b'a', b'b']
        assert ss.zrange(-2, -1) == [b'b', b'c']
    
    def test_zrevrange(self):
        ss = SortedSet()
        ss.zadd(1.0, b'a')
        ss.zadd(2.0, b'b')
        ss.zadd(3.0, b'c')
        
        assert ss.zrevrange(0, -1) == [b'c', b'b', b'a']
    
    def test_zrangebyscore(self):
        ss = SortedSet()
        for i in range(10):
            ss.zadd(float(i), f'm{i}'.encode())
        
        result = ss.zrangebyscore(3.0, 7.0)
        expected = [f'm{i}'.encode() for i in range(3, 8)]
        assert result == expected
    
    def test_zrangebyscore_with_limit(self):
        ss = SortedSet()
        for i in range(10):
            ss.zadd(float(i), f'm{i}'.encode())
        
        result = ss.zrangebyscore(0.0, 9.0, offset=2, limit=3)
        assert result == [b'm2', b'm3', b'm4']
    
    def test_zrevrangebyscore(self):
        ss = SortedSet()
        for i in range(5):
            ss.zadd(float(i), f'm{i}'.encode())
        
        result = ss.zrevrangebyscore(4.0, 0.0)
        assert result == [b'm4', b'm3', b'm2', b'm1', b'm0']
    
    def test_zcount(self):
        ss = SortedSet()
        for i in range(10):
            ss.zadd(float(i), f'm{i}'.encode())
        
        assert ss.zcount(3.0, 7.0) == 5
        assert ss.zcount(0.0, 9.0) == 10
    
    def test_zrank(self):
        ss = SortedSet()
        ss.zadd(10.0, b'a')
        ss.zadd(20.0, b'b')
        ss.zadd(30.0, b'c')
        
        assert ss.zrank(b'a') == 0
        assert ss.zrank(b'b') == 1
        assert ss.zrank(b'c') == 2
        assert ss.zrank(b'missing') is None
    
    def test_zrevrank(self):
        ss = SortedSet()
        ss.zadd(10.0, b'a')
        ss.zadd(20.0, b'b')
        ss.zadd(30.0, b'c')
        
        assert ss.zrevrank(b'c') == 0
        assert ss.zrevrank(b'a') == 2
    
    def test_serialization(self):
        ss = SortedSet()
        ss.zadd(1.5, b'alpha')
        ss.zadd(2.5, b'beta')
        ss.zadd(3.5, b'gamma')
        
        data = ss.serialize()
        ss2 = SortedSet.deserialize(data)
        
        assert ss2.zcard() == 3
        assert ss2.zscore(b'beta') == 2.5
        assert ss2.zrange(0, -1) == [b'alpha', b'beta', b'gamma']
    
    def test_empty_set(self):
        ss = SortedSet()
        assert ss.zrange(0, -1) == []
        assert ss.zrangebyscore(0, 100) == []
        assert ss.zcount(0, 100) == 0
