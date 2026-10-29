"""Tests for SSTable"""
import pytest
import tempfile
import os
from ignisdb_v2.memtable import SkipList
from ignisdb_v2.sstable import SSTable


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def sample_memtable():
    sl = SkipList()
    for i in range(100):
        sl.put(f'key:{i:04d}'.encode(), f'value:{i}'.encode())
    return sl


class TestSSTable:
    @pytest.mark.asyncio
    async def test_create_and_read(self, temp_dir, sample_memtable):
        sst = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        
        # Verify all keys readable
        for i in range(100):
            key = f'key:{i:04d}'.encode()
            expected = f'value:{i}'.encode()
            assert sst.get(key) == expected, f"Failed for key {key}"
        
        sst.close()
    
    @pytest.mark.asyncio
    async def test_missing_key(self, temp_dir, sample_memtable):
        sst = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        
        assert sst.get(b'nonexistent') is None
        assert sst.get(b'key:9999') is None
        
        sst.close()
    
    @pytest.mark.asyncio
    async def test_bloom_filter(self, temp_dir, sample_memtable):
        sst = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        
        # Existing keys should pass bloom filter
        assert sst.might_contain(b'key:0050')
        
        # Non-existing keys should mostly fail bloom filter
        # (some false positives allowed)
        false_positives = 0
        for i in range(1000):
            if sst.might_contain(f'missing:{i}'.encode()):
                false_positives += 1
        
        # FP rate should be low (< 5%)
        assert false_positives < 50
        
        sst.close()
    
    @pytest.mark.asyncio
    async def test_iter_all(self, temp_dir, sample_memtable):
        sst = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        
        items = list(sst.iter_all())
        assert len(items) == 100
        
        # Verify sorted order
        keys = [k for k, v in items]
        assert keys == sorted(keys)
        
        sst.close()
    
    @pytest.mark.asyncio
    async def test_entry_count(self, temp_dir, sample_memtable):
        sst = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        
        assert sst.entry_count == 100
        
        sst.close()
    
    @pytest.mark.asyncio
    async def test_reopen(self, temp_dir, sample_memtable):
        # Create SSTable
        sst1 = await SSTable.create(sample_memtable, temp_dir, sequence=1)
        path = str(sst1.path)
        sst1.close()
        
        # Reopen
        sst2 = SSTable(path)
        sst2.open()
        
        assert sst2.get(b'key:0050') == b'value:50'
        assert sst2.entry_count == 100
        
        sst2.close()
