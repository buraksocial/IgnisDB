"""Tests for the append-only file: replay, rewrite, and failure handling."""
import asyncio

from ignisdb.persistence import AofHandler, iter_restore_commands
from ignisdb.storage import StorageEngine


def run(coro):
    return asyncio.run(coro)


def test_write_and_replay_roundtrip(tmp_path):
    path = str(tmp_path / "test.aof")

    async def scenario():
        handler = AofHandler(path)
        handler.open()
        handler.write("SET", "k", "v")
        handler.write("SADD", "s", "a", "b")
        await handler.fsync()
        handler.close()

        assert AofHandler(path).load() == [
            ("SET", ["k", "v"]),
            ("SADD", ["s", "a", "b"]),
        ]

    run(scenario())


def test_rewrite_compacts_repeated_writes(tmp_path):
    path = str(tmp_path / "test.aof")

    async def scenario():
        handler = AofHandler(path)
        handler.open()
        for i in range(20):
            handler.write("SET", "k", str(i))
        await handler.fsync()

        storage = StorageEngine()
        await storage.set("k", "19")
        assert await handler.rewrite(storage.snapshot()) is True
        handler.close()

        assert AofHandler(path).load() == [("SET", ["k", "19"])]

    run(scenario())


def test_rewrite_does_not_replay_the_stale_buffer(tmp_path):
    """The rewrite already encodes current state.

    Flushing the pending buffer into the compacted file duplicated every
    buffered LPUSH/SADD member on the next restart.
    """
    path = str(tmp_path / "test.aof")

    async def scenario():
        handler = AofHandler(path)
        handler.open()
        handler.write("SADD", "s", "a")  # still buffered, not yet on disk

        storage = StorageEngine()
        await storage.sadd("s", ["a"])
        await handler.rewrite(storage.snapshot())
        await handler.fsync()
        handler.close()

        replayed = StorageEngine()
        for command, args in AofHandler(path).load():
            if command == "SADD":
                await replayed.sadd(args[0], args[1:])
        assert sorted(await replayed.smembers("s")) == ["a"]

    run(scenario())


def test_rewrite_failure_on_a_closed_handler_reports_false(tmp_path):
    """A failure before the swap must return False, not raise UnboundLocalError.

    `was_running` was read in the except block but assigned only after the temp
    file had been written. On a handler that is not currently open - after
    close(), or when open() failed - the recovery branch reads it and raises a
    second, misleading exception that buries the real error.
    """
    path = str(tmp_path / "test.aof")

    async def scenario():
        handler = AofHandler(path)  # deliberately not opened

        class Exploding(dict):
            def items(self):
                raise IOError("disk on fire")

        assert await handler.rewrite(Exploding()) is False

    run(scenario())


def test_rewrite_failure_on_an_open_handler_reports_false(tmp_path):
    path = str(tmp_path / "test.aof")

    async def scenario():
        handler = AofHandler(path)
        handler.open()

        class Exploding(dict):
            def items(self):
                raise IOError("disk on fire")

        assert await handler.rewrite(Exploding()) is False
        handler.close()

    run(scenario())


def test_rewrite_keeps_the_old_file_on_failure(tmp_path):
    path = tmp_path / "test.aof"

    async def scenario():
        handler = AofHandler(str(path))
        handler.open()
        handler.write("SET", "k", "v")
        await handler.fsync()
        before = path.read_text(encoding="utf-8")

        class Exploding(dict):
            def items(self):
                raise IOError("disk on fire")

        await handler.rewrite(Exploding())
        handler.close()

        assert path.read_text(encoding="utf-8") == before
        assert not (tmp_path / "test.aof.rewrite").exists()

    run(scenario())


def test_restore_commands_cover_every_type():
    data = {
        "s": ("string", "v", None),
        "l": ("list", ["a", "b"], None),
        "h": ("hash", {"f": "v"}, None),
        "st": ("set", {"m"}, None),
    }
    commands = {cmd for cmd, _ in iter_restore_commands(data)}
    assert commands == {"SET", "LPUSH", "HSET", "SADD"}


def test_restore_commands_preserve_list_order():
    data = {"l": ("list", ["a", "b", "c"], None)}

    async def scenario():
        storage = StorageEngine()
        for command, args in iter_restore_commands(data):
            if command == "LPUSH":
                await storage.lpush(args[0], args[1:])
        assert await storage.lrange("l", 0, -1) == ["a", "b", "c"]

    run(scenario())


def test_restore_commands_skip_empty_containers():
    """`LPUSH key` with no members is a syntax error on replay."""
    data = {"l": ("list", [], None), "st": ("set", set(), None)}
    assert list(iter_restore_commands(data)) == []


def test_restore_commands_keep_hash_ttls():
    """The TTL was skipped for hashes, so a rewrite made expiring hashes permanent."""
    import time
    data = {"h": ("hash", {"f": "v"}, time.time() + 120)}
    commands = [cmd for cmd, _ in iter_restore_commands(data)]
    assert "EXPIRE" in commands


def test_restore_commands_drop_expired_keys():
    import time
    data = {"gone": ("string", "v", time.time() - 1)}
    assert list(iter_restore_commands(data)) == []
