"""Tests for snapshot and AOF persistence."""
import asyncio
import json
import os

from ignisdb.persistence import SnapshotHandler, periodic_snapshot
from ignisdb.storage import StorageEngine


def run(coro):
    return asyncio.run(coro)


def test_snapshot_roundtrip_preserves_types(tmp_path):
    """Every supported value type must survive a save/load cycle unchanged."""
    path = str(tmp_path / "snap.json")
    handler = SnapshotHandler(path)

    original = {
        "str": ("string", "hello", None),
        "list": ("list", ["a", "b"], None),
        "hash": ("hash", {"f": "v"}, None),
        "set": ("set", {"m1", "m2"}, None),
    }
    handler.save(original)
    loaded = handler.load()

    assert loaded["str"] == ("string", "hello", None)
    assert loaded["list"] == ("list", ["a", "b"], None)
    assert loaded["hash"] == ("hash", {"f": "v"}, None)
    assert loaded["set"] == ("set", {"m1", "m2"}, None)


def test_reloaded_set_still_accepts_sadd(tmp_path):
    """JSON has no set type, so sets are written as arrays.

    If load() hands back a list, the key looks like a set but the next SADD
    raises AttributeError: 'list' object has no attribute 'add'.
    """
    path = str(tmp_path / "snap.json")
    handler = SnapshotHandler(path)
    handler.save({"s": ("set", {"a"}, None)})

    async def scenario():
        storage = StorageEngine()
        await storage.load_data(handler.load())
        assert await storage.sadd("s", ["b"]) == 1
        assert sorted(await storage.smembers("s")) == ["a", "b"]

    run(scenario())


def test_snapshot_load_missing_file_is_empty(tmp_path):
    handler = SnapshotHandler(str(tmp_path / "does_not_exist.json"))
    assert handler.load() == {}


def test_snapshot_save_is_atomic(tmp_path):
    """A failed save must not destroy the previous snapshot."""
    path = tmp_path / "snap.json"
    handler = SnapshotHandler(str(path))
    handler.save({"good": ("string", "value", None)})
    before = path.read_text(encoding="utf-8")

    # A set is not JSON-serialisable once it reaches json.dump directly, so
    # this save fails partway through.
    handler.save({"bad": ("string", object(), None)})

    assert path.read_text(encoding="utf-8") == before
    assert not (tmp_path / "snap.json.tmp").exists()


def test_periodic_snapshot_writes_to_disk(tmp_path):
    """Regression test: the snapshot loop must actually persist data.

    It previously called a StorageEngine method that does not exist, so the
    task died on its first tick with an AttributeError that was never
    retrieved, and 'snapshot' mode silently never wrote a file.
    """
    path = tmp_path / "snap.json"

    async def scenario():
        storage = StorageEngine()
        await storage.set("persisted", "yes")
        handler = SnapshotHandler(str(path))

        task = asyncio.create_task(periodic_snapshot(storage, handler, 0))
        for _ in range(50):
            await asyncio.sleep(0.01)
            if path.exists():
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    run(scenario())

    assert path.exists(), "periodic_snapshot never wrote a snapshot file"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["persisted"] == ["string", "yes", None]


def test_periodic_snapshot_survives_a_failed_save(tmp_path):
    """One bad save must not silently stop all future snapshots."""
    calls = []

    class FlakyHandler:
        def save(self, data):
            calls.append(data)
            if len(calls) == 1:
                raise IOError("disk on fire")

    async def scenario():
        storage = StorageEngine()
        await storage.set("k", "v")
        handler = FlakyHandler()
        task = asyncio.create_task(periodic_snapshot(storage, handler, 0))
        for _ in range(50):
            await asyncio.sleep(0.01)
            if len(calls) >= 2:
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    run(scenario())

    assert len(calls) >= 2, "snapshot loop stopped after the first failure"
