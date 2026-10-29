# 🔥 IgnisDB v2: LSM-Tree + Tiered Storage Database

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

> High-performance key-value database with LSM-Tree storage engine, tiered caching,
> and unlimited storage capacity — all in pure Python.

## What is IgnisDB?

IgnisDB is a key-value database built from scratch in Python. **v2** introduces a
Log-Structured Merge Tree (LSM-Tree) storage engine that keeps RAM usage fixed at
64–128 MB regardless of dataset size, while providing unlimited disk-backed storage.

It supports two modes:
- **Embedded**: Direct API calls, zero network overhead
- **Server**: RESP protocol TCP server, backward compatible with v1 clients

## Performance Benchmarks

Benchmarked on Linux x86_64, 100K keys × 100 bytes each:

| Metric | Result |
|---|---|
| **Write throughput** | **20,037 ops/sec** |
| **Read throughput (hot cache)** | **857,382 ops/sec** |
| **Read throughput (sequential)** | **963,234 ops/sec** |
| **ZADD (sorted set)** | **1,427,362 ops/sec** |
| **ZRANGEBYSCORE** | **6,902 ops/sec** |
| **Full-text indexing** | **196,289 docs/sec** |
| **Full-text search** | **15,031 searches/sec** |
| **RAM usage (100K keys)** | **90.5 MB** |
| **Cache hit rate** | **100%** |

### Memory Efficiency

| Dataset Size | RAM Usage | Notes |
|---|---|---|
| 100K keys (10 MB data) | ~90 MB | Hot cache + MemTable |
| 1M keys (100 MB data) | ~128 MB | SSTables on disk, cache capped |
| 10M keys (1 GB data) | ~128 MB | Same RAM, unlimited disk |

> **Key insight**: Unlike v1 where 2 GB of data required 2 GB of RAM, v2 maintains
> constant memory usage through tiered storage. Cold data lives on disk via mmap'd
> SSTables; only frequently accessed keys occupy the hot LRU cache.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Client / API                       │
├─────────────────────────────────────────────────────┤
│              RESP Protocol Server                    │
│         (backward compatible with v1)                │
├──────────┬──────────────────────────────────────────┤
│ Hot Cache│  LRU Cache (64 MB fixed)                 │
│          │  Most frequently accessed keys             │
├──────────┼──────────────────────────────────────────┤
│ MemTable │  SkipList (16 MB)                         │
│          │  Recent writes, sorted in-memory           │
├──────────┼──────────────────────────────────────────┤
│ WAL      │  Write-Ahead Log                          │
│          │  Crash recovery guarantee                  │
├──────────┼──────────────────────────────────────────┤
│ SSTables │  Sorted String Tables (disk, mmap'd)      │
│          │  Bloom filter + sparse index               │
│          │  Background compaction                     │
└──────────┴──────────────────────────────────────────┘
```

### Write Path
1. Key-value → **WAL** append (crash safety)
2. Key-value → **MemTable** (SkipList, sorted)
3. When MemTable reaches 16 MB → flush to **SSTable** on disk
4. Background **compaction** merges old SSTables

### Read Path
1. Check **Hot Cache** (LRU, ~0.001ms)
2. Check **MemTable** (~0.005ms)
3. Check **Immutable MemTables** (flush queue)
4. Check **SSTables** newest-first (bloom filter skip, ~2-5ms)

## Quick Start

### Installation

```bash
git clone https://github.com/buraksocial/IgnisDB.git
cd IgnisDB
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Embedded Mode (Recommended)

```python
import asyncio
from ignisdb_v2 import DB

async def main():
    async with await DB.open('/var/lib/ignisdb') as db:
        # Basic key-value
        await db.set(b'name', b'IgnisDB v2')
        value = await db.get(b'name')
        print(value)  # b'IgnisDB v2'

        # String keys also work
        await db.set('description', 'LSM-Tree based KV store')

        # Sorted Sets
        engine = db.engine
        engine.zadd(b'leaderboard', 100.0, b'alice')
        engine.zadd(b'leaderboard', 200.0, b'bob')
        top = engine.zrevrange(b'leaderboard', 0, 2)
        print(top)  # [b'bob', b'alice']

        # Full-Text Search
        engine.ft_add('post:1', 'Introduction to Python programming')
        engine.ft_add('post:2', 'Advanced Python decorators')
        results = engine.ft_search('python', limit=5)
        print(results)  # [('post:1', score), ('post:2', score)]

asyncio.run(main())
```

### Server Mode (Backward Compatible)

```python
import asyncio
from ignisdb_v2 import DB, Server

async def main():
    db = await DB.open('/var/lib/ignisdb')
    server = Server(db.engine, host='localhost', port=6380)
    await server.serve()  # RESP protocol, works with any Redis client

asyncio.run(main())
```

Connect with telnet or any Redis-compatible client:
```bash
telnet localhost 6380
SET name "IgnisDB"
GET name
```

## Supported Commands

### String Operations (v1 compatible)
| Command | Description |
|---|---|
| `GET key` | Get value by key |
| `SET key value [EX seconds]` | Set key-value with optional TTL |
| `DEL key [key ...]` | Delete one or more keys |
| `EXISTS key` | Check if key exists |
| `EXPIRE key seconds` | Set TTL on a key |
| `TTL key` | Get remaining TTL |
| `PERSIST key` | Remove TTL from key |

### List Operations (v1 compatible)
| Command | Description |
|---|---|
| `LPUSH key value [value ...]` | Push values to head of list |
| `RPUSH key value [value ...]` | Push values to tail of list |
| `LRANGE key start stop` | Get range of list elements |
| `LLEN key` | Get list length |

### Hash Operations (v1 compatible)
| Command | Description |
|---|---|
| `HSET key field value` | Set hash field |
| `HGET key field` | Get hash field |
| `HGETALL key` | Get all fields and values |
| `HDEL key field [field ...]` | Delete hash fields |

### Set Operations (v1 compatible)
| Command | Description |
|---|---|
| `SADD key member [member ...]` | Add members to set |
| `SMEMBERS key` | Get all members |
| `SISMEMBER key member` | Check membership |
| `SREM key member [member ...]` | Remove members |

### Sorted Set Operations (v2 NEW)
| Command | Description |
|---|---|
| `ZADD key score member [score member ...]` | Add/update members with scores |
| `ZRANGE key start stop` | Range by rank (ascending) |
| `ZREVRANGE key start stop` | Range by rank (descending) |
| `ZRANGEBYSCORE key min max [LIMIT offset count]` | Range by score |
| `ZREVRANGEBYSCORE key max min [LIMIT offset count]` | Reverse range by score |
| `ZSCORE key member` | Get member's score |
| `ZREM key member [member ...]` | Remove members |
| `ZCARD key` | Get cardinality |

### Full-Text Search (v2 NEW)
| Command | Description |
|---|---|
| `FT.ADD doc_id text` | Index a document |
| `FT.SEARCH query [LIMIT count]` | Search with TF-IDF ranking |

### Utility
| Command | Description |
|---|---|
| `PING [message]` | Ping server |
| `INFO` | Server statistics |
| `CACHE.STATS` | Cache hit/miss statistics |
| `AUTH password` | Authentication |

## Migration Guide: v1 → v2

### For Application Developers

#### Embedded Mode (New, Recommended)
v2 introduces an embedded mode that eliminates network overhead entirely:

```python
# ❌ v1: Network-only
import asyncio
from ignisdb import Client

async def main():
    client = await Client.connect('localhost:6380')
    await client.set('key', 'value')
    value = await client.get('key')
```

```python
# ✅ v2: Embedded (no network, direct function calls)
import asyncio
from ignisdb_v2 import DB

async def main():
    async with await DB.open('/var/lib/ignisdb') as db:
        await db.set('key', 'value')
        value = await db.get('key')
```

#### Server Mode (Drop-in Replacement)
If you prefer the client-server model, v2's server is fully backward compatible:

```python
# v2 server — same port, same protocol, same commands
from ignisdb_v2 import DB, Server

db = await DB.open('/var/lib/ignisdb')
server = Server(db.engine, host='localhost', port=6380)
await server.serve()
```

Your existing v1 clients continue to work without any changes.

### Key Differences

| Aspect | v1 | v2 |
|---|---|---|
| Storage | All data in RAM | LSM-Tree (RAM + disk) |
| RAM usage | Linear with data | Fixed 64–128 MB |
| Data capacity | Limited by RAM | Unlimited (disk) |
| Persistence | Snapshot + AOF | WAL + SSTables |
| Crash recovery | Replay AOF | Replay WAL (faster) |
| Sorted sets | ❌ | ✅ ZADD/ZRANGE/etc. |
| Full-text search | ❌ | ✅ FT.ADD/FT.SEARCH |
| Secondary indexes | ❌ | ✅ Score-based ordering |
| Embedded mode | ❌ | ✅ Zero network overhead |
| RESP compatibility | ✅ | ✅ Backward compatible |

### Breaking Changes

There are **no breaking changes** for server-mode users. The RESP protocol and all
v1 commands work identically.

For embedded mode users migrating from direct `StorageEngine` access:
- `StorageEngine` → `LSMEngine` / `DB`
- All methods are now `async`
- Keys and values are `bytes` (strings auto-encoded)

## Project Structure

```
IgnisDB/
├── ignisdb/                  # v1 source (preserved on v1 branch)
│   ├── storage.py            # In-memory storage engine
│   ├── protocol.py           # RESP parser
│   ├── server.py             # TCP server
│   ├── persistence.py        # AOF + Snapshot
│   └── ...
├── ignisdb_v2/               # v2 source
│   ├── __init__.py           # Package exports
│   ├── engine.py             # LSMEngine + DB API
│   ├── memtable.py           # SkipList implementation
│   ├── wal.py                # Write-Ahead Log
│   ├── sstable.py            # SSTable + mmap
│   ├── cache.py              # TieredCache (LRU)
│   ├── bloom.py              # BloomFilter
│   ├── sorted_set.py         # SortedSet (ZADD/ZRANGE)
│   ├── search.py             # InvertedIndex (full-text)
│   └── server.py             # RESP server wrapper
├── tests/                    # Test suite (63 tests)
│   ├── test_bloom.py
│   ├── test_memtable.py
│   ├── test_sstable.py
│   ├── test_engine.py
│   ├── test_cache.py
│   ├── test_sorted_set.py
│   └── test_search.py
├── benchmarks/
│   └── mixed_workload.py     # Comprehensive benchmark
├── examples/
│   └── basic_usage.py        # Usage examples
├── README.md
├── requirements.txt
└── LICENSE
```

## Running Tests

```bash
pip install pytest pytest-asyncio
python -m pytest tests/ -v
```

## Running Benchmarks

```bash
python benchmarks/mixed_workload.py
```

## Configuration

Default settings can be overridden when opening the database:

```python
db = await DB.open(
    '/var/lib/ignisdb',
    memtable_size=16 * 1024 * 1024,   # 16 MB MemTable flush threshold
    cache_size=64 * 1024 * 1024,       # 64 MB hot cache
)
```

## Version History

- **v2.0** — LSM-Tree engine, tiered storage, sorted sets, full-text search
- **v1.x** — In-memory asyncio KV store with RESP protocol ([v1 branch](../../tree/v1))

## License

MIT License. See [LICENSE](LICENSE).

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## References

- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [LevelDB Documentation](https://github.com/google/leveldb/blob/main/doc/index.md)
- [LSM-Tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Bloom Filter (Wikipedia)](https://en.wikipedia.org/wiki/Bloom_filter)
