"""Tests for LSMEngine"""
import pytest
import pytest_asyncio
import tempfile
import asyncio
from ignisdb_v2.engine import LSMEngine, DB


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest_asyncio.fixture
async def engine(temp_dir):
    eng = LSMEngine(temp_dir, memtable_size=1024)  # Small for testing
    await eng.open()
    yield eng
    await eng.close()


class TestLSMEngine:
    @pytest.mark.asyncio
    async def test_put_get(self, engine):
        await engine.put(b'key1', b'value1')
        result = await engine.get(b'key1')
        assert result == b'value1'
    
    @pytest.mark.asyncio
    async def test_overwrite(self, engine):
        await engine.put(b'key', b'old')
        await engine.put(b'key', b'new')
        assert await engine.get(b'key') == b'new'
    
    @pytest.mark.asyncio
    async def test_delete(self, engine):
        await engine.put(b'key', b'value')
        await engine.delete(b'key')
        assert await engine.get(b'key') is None
    
    @pytest.mark.asyncio
    async def test_missing_key(self, engine):
        assert await engine.get(b'nonexistent') is None
    
    @pytest.mark.asyncio
    async def test_many_keys_trigger_flush(self, engine):
        # Write enough to trigger flush (memtable_size=1024)
        for i in range(200):
            await engine.put(f'key:{i:04d}'.encode(), (f'value:{i}' * 10).encode())
        
        # Wait for background flush
        await asyncio.sleep(0.5)
        
        # All keys should still be readable
        for i in range(200):
            result = await engine.get(f'key:{i:04d}'.encode())
            assert result is not None, f"Key key:{i:04d} not found"
    
    @pytest.mark.asyncio
    async def test_stats(self, engine):
        await engine.put(b'key', b'value')
        await engine.get(b'key')
        
        stats = engine.get_stats()
        assert stats['writes'] >= 1
        assert stats['reads'] >= 1
        assert 'cache' in stats
    
    @pytest.mark.asyncio
    async def test_sorted_set_operations(self, engine):
        engine.zadd(b'myset', 1.0, b'member1')
        engine.zadd(b'myset', 2.0, b'member2')
        engine.zadd(b'myset', 3.0, b'member3')
        
        assert engine.zcard(b'myset') == 3
        assert engine.zscore(b'myset', b'member2') == 2.0
        
        result = engine.zrange(b'myset', 0, -1)
        assert result == [b'member1', b'member2', b'member3']
        
        result = engine.zrevrange(b'myset', 0, -1)
        assert result == [b'member3', b'member2', b'member1']
    
    @pytest.mark.asyncio
    async def test_fulltext_search(self, engine):
        engine.ft_add('doc1', 'The quick brown fox jumps over the lazy dog')
        engine.ft_add('doc2', 'A fast brown fox leaps over a sleepy cat')
        engine.ft_add('doc3', 'Python programming is fun and educational')
        
        results = engine.ft_search('fox', limit=10)
        doc_ids = [doc_id for doc_id, score in results]
        assert 'doc1' in doc_ids
        assert 'doc2' in doc_ids
        assert 'doc3' not in doc_ids


class TestDB:
    @pytest.mark.asyncio
    async def test_context_manager(self, temp_dir):
        async with await DB.open(temp_dir) as db:
            await db.set(b'key', b'value')
            result = await db.get(b'key')
            assert result == b'value'
    
    @pytest.mark.asyncio
    async def test_string_keys(self, temp_dir):
        async with await DB.open(temp_dir) as db:
            await db.set('string_key', 'string_value')
            result = await db.get('string_key')
            assert result == b'string_value'
