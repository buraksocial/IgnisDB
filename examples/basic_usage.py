#!/usr/bin/env python3
"""
IgnisDB v2 Basic Usage Example

Demonstrates embedded mode and server mode usage.
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ignisdb import DB, Server


async def embedded_example():
    """Embedded mode: direct API calls, no network overhead."""
    print("\n=== Embedded Mode ===\n")
    
    async with await DB.open('/tmp/ignisdb_example') as db:
        # Basic key-value operations
        await db.set(b'name', b'IgnisDB v2')
        await db.set(b'version', b'2.0.0')
        
        name = await db.get(b'name')
        print(f"GET name: {name}")
        
        # String keys also work
        await db.set('description', 'LSM-Tree based KV store')
        desc = await db.get('description')
        print(f"GET description: {desc}")
        
        # Delete
        await db.set(b'temp', b'data')
        await db.delete(b'temp')
        result = await db.get(b'temp')
        print(f"GET temp (after delete): {result}")
        
        # Sorted Sets
        engine = db.engine
        engine.zadd(b'leaderboard', 100.0, b'alice')
        engine.zadd(b'leaderboard', 200.0, b'bob')
        engine.zadd(b'leaderboard', 150.0, b'charlie')
        
        top = engine.zrevrange(b'leaderboard', 0, 2)
        print(f"\nLeaderboard (top 3): {[m.decode() for m in top]}")
        
        # Range query
        mid = engine.zrangebyscore(b'leaderboard', 100, 160)
        print(f"Scores 100-160: {[m.decode() for m in mid]}")
        
        # Full-Text Search
        engine.ft_add('post:1', 'Introduction to Python programming')
        engine.ft_add('post:2', 'Advanced Python decorators and metaclasses')
        engine.ft_add('post:3', 'JavaScript async await patterns')
        
        results = engine.ft_search('python', limit=5)
        print(f"\nSearch 'python': {[(d, round(s, 3)) for d, s in results]}")
        
        # Stats
        stats = db.stats()
        print(f"\nStats: writes={stats['writes']}, reads={stats['reads']}, "
              f"cache_hit_rate={stats['cache']['hit_rate_pct']}%")


async def server_example():
    """Server mode: RESP protocol, backward compatible with v1 clients."""
    print("\n=== Server Mode ===\n")
    print("Starting server on localhost:6380...")
    print("Connect with: telnet localhost 6380")
    print("Or use any Redis-compatible client\n")
    
    from ignisdb.engine import LSMEngine
    
    engine = LSMEngine('/tmp/ignisdb_server')
    await engine.open()
    
    server = Server(engine, host='localhost', port=6380)
    
    try:
        await server.serve()
    except KeyboardInterrupt:
        print("\nShutting down...")
        await server.stop()


async def forum_tutorial_example():
    """ForumTutorial integration example."""
    print("\n=== ForumTutorial Integration ===\n")
    
    async with await DB.open('/tmp/ignisdb_forum') as db:
        engine = db.engine
        import time
        
        # Store forum categories
        await db.set(b'category:1:name', b'General Discussion')
        await db.set(b'category:2:name', b'Technical Support')
        
        # Store threads with sorted set for ordering
        threads = [
            ('thread:1', 'Welcome to the forum!', 1, time.time() - 3600),
            ('thread:2', 'How to use IgnisDB?', 1, time.time() - 1800),
            ('thread:3', 'Bug report: login issue', 2, time.time() - 900),
            ('thread:4', 'Feature request: dark mode', 1, time.time() - 300),
        ]
        
        for thread_id, title, category_id, timestamp in threads:
            await db.set(f'{thread_id}:title'.encode(), title.encode())
            await db.set(f'{thread_id}:category'.encode(), str(category_id).encode())
            # Index by category with timestamp score
            engine.zadd(f'category:{category_id}:threads'.encode(), 
                       timestamp, thread_id.encode())
        
        # Get latest threads in category 1
        cat1_threads = engine.zrevrangebyscore(
            b'category:1:threads', float('inf'), 0, 0, 10
        )
        
        print("Latest threads in 'General Discussion':")
        for thread_id in cat1_threads:
            title = await db.get(f'{thread_id.decode()}:title'.encode())
            print(f"  - {title.decode()}")
        
        # Full-text search across threads
        for thread_id, title, _, _ in threads:
            engine.ft_add(thread_id, title)
        
        results = engine.ft_search('forum ignisdb', limit=5)
        print(f"\nSearch 'forum ignisdb': {[d for d, s in results]}")


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'embedded'
    
    if mode == 'server':
        await server_example()
    elif mode == 'forum':
        await forum_tutorial_example()
    else:
        await embedded_example()
        await forum_tutorial_example()


if __name__ == '__main__':
    asyncio.run(main())
