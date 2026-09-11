"""Tests for master -> replica replication."""
import asyncio
import contextlib

from ignisdb.server import IgnisServer


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


def make_server(**kwargs):
    options = dict(
        host="127.0.0.1",
        port=0,
        persistence_mode="none",
        snapshot_path="unused.json",
        aof_path="unused.aof",
        snapshot_interval=0,
    )
    options.update(kwargs)
    return IgnisServer(**options)


@contextlib.asynccontextmanager
async def running_server(server):
    await server.initialize()
    tcp = await asyncio.start_server(server.handle_client, server.host, 0)
    try:
        yield tcp.sockets[0].getsockname()[1]
    finally:
        tcp.close()
        await tcp.wait_closed()
        server.shutdown()


async def send(port, raw: bytes):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    await asyncio.sleep(0.05)
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()


async def wait_for(check, timeout=5.0):
    """Polls until `check()` is truthy, so tests do not race the event loop."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.02)
    return None


def test_replica_receives_writes_made_after_it_connects():
    """Regression test: replication never started at all.

    The replica sent the literal characters `SYNC\\r\\n` - an escaped
    backslash-r, not a carriage return - so the master's framer never saw a
    complete command and the replica was never registered.
    """
    async def scenario():
        master, replica = make_server(), make_server()
        async with running_server(master) as master_port, running_server(replica) as replica_port:
            task = asyncio.create_task(replica.connect_to_master("127.0.0.1", master_port))
            await wait_for(lambda: asyncio.sleep(0, result=bool(master.replicas)))
            assert master.replicas, "master never registered the replica"

            reader, writer = await asyncio.open_connection("127.0.0.1", master_port)
            writer.write(b"SET shared value\r\n")
            await writer.drain()
            await reader.readuntil(b"\r\n")

            found = await wait_for(lambda: replica.storage.get("shared"))
            assert found == "value"

            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    run(scenario())


def test_replica_receives_the_existing_dataset_on_sync():
    """A replica attaching to a non-empty master must not start blank."""
    async def scenario():
        master, replica = make_server(), make_server()
        await master.storage.set("before", "existing")
        await master.storage.lpush("mylist", ["b", "a"])
        await master.storage.hset("myhash", "field", "hv")
        await master.storage.sadd("myset", ["m1"])

        async with running_server(master) as master_port, running_server(replica) as replica_port:
            task = asyncio.create_task(replica.connect_to_master("127.0.0.1", master_port))

            assert await wait_for(lambda: replica.storage.get("before")) == "existing"
            assert await replica.storage.lrange("mylist", 0, -1) == ["a", "b"]
            assert await replica.storage.hget("myhash", "field") == "hv"
            assert await replica.storage.smembers("myset") == ["m1"]

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    run(scenario())


def test_replicated_value_containing_spaces_is_not_split():
    """Inline framing turned `hello world` into two arguments; RESP does not."""
    async def scenario():
        master, replica = make_server(), make_server()
        async with running_server(master) as master_port, running_server(replica) as replica_port:
            task = asyncio.create_task(replica.connect_to_master("127.0.0.1", master_port))
            await wait_for(lambda: asyncio.sleep(0, result=bool(master.replicas)))

            await send(master_port, b"*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$11\r\nhello world\r\n")

            found = await wait_for(lambda: replica.storage.get("greeting"))
            assert found == "hello world", f"replica stored {found!r}"

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    run(scenario())


def test_replicated_value_has_no_trailing_escape_characters():
    """The old inline payload appended a literal backslash-r to every value."""
    async def scenario():
        master, replica = make_server(), make_server()
        async with running_server(master) as master_port, running_server(replica) as replica_port:
            task = asyncio.create_task(replica.connect_to_master("127.0.0.1", master_port))
            await wait_for(lambda: asyncio.sleep(0, result=bool(master.replicas)))

            await send(master_port, b"SET k clean\r\n")

            found = await wait_for(lambda: replica.storage.get("k"))
            assert found == "clean"
            assert "\\" not in found

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    run(scenario())


def test_disconnected_replica_is_unregistered():
    async def scenario():
        master, replica = make_server(), make_server()
        async with running_server(master) as master_port, running_server(replica) as replica_port:
            task = asyncio.create_task(replica.connect_to_master("127.0.0.1", master_port))
            await wait_for(lambda: asyncio.sleep(0, result=bool(master.replicas)))
            assert master.replicas

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            gone = await wait_for(
                lambda: asyncio.sleep(0, result=not master.replicas)
            )
            assert gone, "master kept writing to a closed replica transport"

    run(scenario())
