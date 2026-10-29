#!/usr/bin/env python3
"""
IgnisDB v2 Benchmark Suite

Measures performance of core operations:
- Write throughput
- Read throughput (hot/cold)
- Sorted set operations
- Memory usage
"""

import asyncio
import time
import sys
import os
import tempfile

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ignisdb import DB


async def benchmark_write(db: DB, n: int = 100_000, value_size: int = 100):
    """Benchmark write performance."""
    value = b'x' * value_size
    
    start = time.time()
    for i in range(n):
        await db.set(f'bench:key:{i:08d}'.encode(), value)
    elapsed = time.time() - start
    
    ops_per_sec = n / elapsed
    print(f"  Write {n:,} keys ({value_size}B each): {elapsed:.2f}s "
          f"({ops_per_sec:,.0f} ops/sec)")
    return ops_per_sec


async def benchmark_read_hot(db: DB, n: int = 100_000):
    """Benchmark hot read performance (data in cache)."""
    # Pre-warm cache
    for i in range(min(n, 1000)):
        await db.get(f'bench:key:{i:08d}'.encode())
    
    start = time.time()
    found = 0
    for i in range(n):
        result = await db.get(f'bench:key:{i % 1000:08d}'.encode())
        if result:
            found += 1
    elapsed = time.time() - start
    
    ops_per_sec = n / elapsed
    print(f"  Read {n:,} keys (hot): {elapsed:.2f}s "
          f"({ops_per_sec:,.0f} ops/sec, {found} found)")
    return ops_per_sec


async def benchmark_read_sequential(db: DB, n: int = 10_000):
    """Benchmark sequential read (may hit SSTables)."""
    start = time.time()
    found = 0
    for i in range(n):
        result = await db.get(f'bench:key:{i:08d}'.encode())
        if result:
            found += 1
    elapsed = time.time() - start
    
    ops_per_sec = n / elapsed
    print(f"  Read {n:,} keys (sequential): {elapsed:.2f}s "
          f"({ops_per_sec:,.0f} ops/sec, {found} found)")
    return ops_per_sec


def benchmark_sorted_set(db: DB, n: int = 10_000):
    """Benchmark sorted set operations."""
    engine = db.engine
    
    # ZADD
    start = time.time()
    for i in range(n):
        engine.zadd(b'bench:zset', float(i), f'member:{i:08d}'.encode())
    zadd_time = time.time() - start
    print(f"  ZADD {n:,} members: {zadd_time:.2f}s ({n/zadd_time:,.0f} ops/sec)")
    
    # ZRANGEBYSCORE
    start = time.time()
    queries = 1000
    for _ in range(queries):
        engine.zrangebyscore(b'bench:zset', 0, n, 0, 20)
    range_time = time.time() - start
    print(f"  ZRANGEBYSCORE {queries:,} queries: {range_time:.2f}s "
          f"({queries/range_time:,.0f} ops/sec)")
    
    # ZREVRANGEBYSCORE
    start = time.time()
    for _ in range(queries):
        engine.zrevrangebyscore(b'bench:zset', float(n), 0, 0, 20)
    rev_time = time.time() - start
    print(f"  ZREVRANGEBYSCORE {queries:,} queries: {rev_time:.2f}s "
          f"({queries/rev_time:,.0f} ops/sec)")


def benchmark_fulltext(db: DB, n: int = 1000):
    """Benchmark full-text search."""
    engine = db.engine
    
    # Add documents
    texts = [
        "Python is a versatile programming language used for web development",
        "Machine learning algorithms require large datasets for training",
        "Database optimization techniques improve query performance significantly",
        "Asyncio enables concurrent programming in Python applications",
        "LSM trees provide efficient write-heavy workload performance",
    ]
    
    start = time.time()
    for i in range(n):
        text = texts[i % len(texts)] + f" document {i}"
        engine.ft_add(f'doc:{i}', text)
    add_time = time.time() - start
    print(f"  FT.ADD {n:,} documents: {add_time:.2f}s ({n/add_time:,.0f} docs/sec)")
    
    # Search
    queries = ["python", "database", "machine learning", "performance", "asyncio"]
    start = time.time()
    search_count = 100
    for _ in range(search_count):
        for q in queries:
            engine.ft_search(q, limit=10)
    search_time = time.time() - start
    total_searches = search_count * len(queries)
    print(f"  FT.SEARCH {total_searches:,} queries: {search_time:.2f}s "
          f"({total_searches/search_time:,.0f} searches/sec)")


def get_memory_usage():
    """Get current process memory usage in MB."""
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        # Fallback: read /proc/self/status on Linux
        try:
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        return int(line.split()[1]) / 1024
        except (FileNotFoundError, ValueError):
            return -1


async def run_benchmark(data_dir: str = None):
    """Run full benchmark suite."""
    if data_dir is None:
        data_dir = tempfile.mkdtemp(prefix='ignisdb_bench_')
    
    print(f"\n{'='*60}")
    print(f"IgnisDB v2 Benchmark")
    print(f"Data directory: {data_dir}")
    print(f"{'='*60}\n")
    
    mem_before = get_memory_usage()
    print(f"Memory before: {mem_before:.1f} MB")
    
    async with await DB.open(data_dir, memtable_size=16*1024*1024, 
                              cache_size=64*1024*1024) as db:
        print("\n--- Write Benchmark ---")
        write_ops = await benchmark_write(db, n=100_000, value_size=100)
        
        print("\n--- Read Benchmark ---")
        read_hot_ops = await benchmark_read_hot(db, n=100_000)
        read_seq_ops = await benchmark_read_sequential(db, n=10_000)
        
        print("\n--- Sorted Set Benchmark ---")
        benchmark_sorted_set(db, n=10_000)
        
        print("\n--- Full-Text Search Benchmark ---")
        benchmark_fulltext(db, n=1_000)
        
        print("\n--- Statistics ---")
        stats = db.stats()
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    {k}: {v}")
            else:
                print(f"  {key}: {value}")
    
    mem_after = get_memory_usage()
    print(f"\nMemory after: {mem_after:.1f} MB")
    print(f"Memory delta: {mem_after - mem_before:.1f} MB")
    
    print(f"\n{'='*60}")
    print("Benchmark complete!")
    print(f"{'='*60}\n")
    
    # Cleanup
    import shutil
    try:
        shutil.rmtree(data_dir)
    except Exception:
        pass


if __name__ == '__main__':
    data_dir = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run_benchmark(data_dir))
