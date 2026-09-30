"""Tests for the in-memory storage engine."""
import asyncio
import time

from ignisdb.storage import StorageEngine


def run(coro):
    """Run a coroutine to completion.

    Deliberately plain asyncio rather than pytest-asyncio, so the suite has no
    plugin dependency and runs on a bare `pip install pytest`.
    """
    return asyncio.run(coro)


def test_set_and_get_roundtrip():
    async def scenario():
        storage = StorageEngine()
        assert await storage.set("name", "John Doe") == "OK"
        assert await storage.get("name") == "John Doe"

    run(scenario())


def test_get_missing_key_returns_none():
    async def scenario():
        storage = StorageEngine()
        assert await storage.get("nope") is None

    run(scenario())


def test_expired_key_is_not_readable():
    async def scenario():
        storage = StorageEngine()
        await storage.set("temp", "value", expire_seconds=-1)
        assert await storage.get("temp") is None

    run(scenario())


def test_delete_expired_key_reports_zero():
    """DEL reports how many keys it actually removed.

    An already-expired key is logically gone, so the answer is 0; reporting 1
    makes the reply indistinguishable from deleting a live key.
    """
    async def scenario():
        storage = StorageEngine()
        await storage.set("temp", "value", expire_seconds=-1)
        assert await storage.delete("temp") == 0

    run(scenario())


def test_delete_live_key_reports_one():
    async def scenario():
        storage = StorageEngine()
        await storage.set("live", "value")
        assert await storage.delete("live") == 1
        assert await storage.delete("live") == 0

    run(scenario())


def test_lpush_prepends_in_reverse_order():
    async def scenario():
        storage = StorageEngine()
        assert await storage.lpush("mylist", ["item1", "item2"]) == 2
        assert await storage.lrange("mylist", 0, -1) == ["item2", "item1"]

    run(scenario())


def test_lrange_negative_stop_excludes_tail():
    async def scenario():
        storage = StorageEngine()
        await storage.lpush("mylist", ["c", "b", "a"])  # -> a, b, c
        assert await storage.lrange("mylist", 0, -1) == ["a", "b", "c"]
        assert await storage.lrange("mylist", 0, -2) == ["a", "b"]

    run(scenario())


def test_sadd_counts_only_new_members():
    async def scenario():
        storage = StorageEngine()
        assert await storage.sadd("s", ["a", "b"]) == 2
        assert await storage.sadd("s", ["b", "c"]) == 1
        assert sorted(await storage.smembers("s")) == ["a", "b", "c"]

    run(scenario())


def test_hset_reports_new_fields_only():
    async def scenario():
        storage = StorageEngine()
        assert await storage.hset("h", "field", "v1") == 1
        assert await storage.hset("h", "field", "v2") == 0
        assert await storage.hget("h", "field") == "v2"

    run(scenario())


def test_snapshot_prunes_expired_keys():
    async def scenario():
        storage = StorageEngine()
        await storage.set("live", "yes")
        await storage.set("dead", "no", expire_seconds=-1)
        snapshot = storage.snapshot()
        assert "live" in snapshot
        assert "dead" not in snapshot

    run(scenario())


def test_snapshot_copies_containers():
    """The snapshot is serialised on a worker thread.

    If it shared list/dict/set objects with live storage, a concurrent write
    would mutate the structure mid-serialisation.
    """
    async def scenario():
        storage = StorageEngine()
        await storage.lpush("mylist", ["a"])
        await storage.hset("myhash", "f", "v")
        await storage.sadd("myset", ["m"])

        snapshot = storage.snapshot()
        await storage.lpush("mylist", ["b"])
        await storage.hset("myhash", "f2", "v2")
        await storage.sadd("myset", ["m2"])

        assert snapshot["mylist"][1] == ["a"]
        assert snapshot["myhash"][1] == {"f": "v"}
        assert snapshot["myset"][1] == {"m"}

    run(scenario())


def test_expire_sets_a_future_deadline():
    async def scenario():
        storage = StorageEngine()
        await storage.set("k", "v")
        assert await storage.expire("k", 60) == 1
        _, _, expires_at = storage.snapshot()["k"]
        assert expires_at is not None and expires_at > time.time()

    run(scenario())


def test_expire_on_missing_key_returns_zero():
    async def scenario():
        storage = StorageEngine()
        assert await storage.expire("ghost", 60) == 0

    run(scenario())
