"""End-to-end tests: a real client speaking RESP to a real server socket."""
import asyncio
import contextlib

from ignisdb.server import IgnisServer


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=10))


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
async def running_server(**kwargs):
    """Binds handle_client to an ephemeral port.

    Deliberately not IgnisServer.start(), which also binds the hard-coded
    MySQL compatibility port 3307 and would clash across tests.
    """
    server = make_server(**kwargs)
    await server.initialize()
    tcp = await asyncio.start_server(server.handle_client, server.host, 0)
    port = tcp.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        tcp.close()
        await tcp.wait_closed()
        server.shutdown()


async def read_reply(reader):
    """Reads exactly one RESP reply.

    Because it trusts the declared bulk length, a reply whose header disagrees
    with its payload shows up here as a mis-read or a timeout.
    """
    line = await reader.readuntil(b"\r\n")
    kind = line[:1]
    if kind in (b"+", b"-", b":"):
        return line
    if kind == b"$":
        length = int(line[1:-2])
        if length == -1:
            return line
        return line + await reader.readexactly(length + 2)
    if kind == b"*":
        count = int(line[1:-2])
        out = line
        for _ in range(count):
            out += await read_reply(reader)
        return out
    return line


class Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def send(self, raw: bytes) -> bytes:
        self.writer.write(raw)
        await self.writer.drain()
        return await read_reply(self.reader)

    async def close(self):
        self.writer.close()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()


@contextlib.asynccontextmanager
async def connect(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    client = Client(reader, writer)
    try:
        yield client
    finally:
        await client.close()


def test_commands_are_case_insensitive():
    """`set foo bar` used to answer -ERR Unknown command 'set'."""
    async def scenario():
        async with running_server() as port, connect(port) as client:
            assert await client.send(b"set foo bar\r\n") == b"+OK\r\n"
            assert await client.send(b"get foo\r\n") == b"$3\r\nbar\r\n"
            assert await client.send(b"GeT foo\r\n") == b"$3\r\nbar\r\n"

    run(scenario())


def test_missing_key_returns_resp_nil():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            assert await client.send(b"GET nothing\r\n") == b"$-1\r\n"

    run(scenario())


def test_non_ascii_value_survives_a_roundtrip():
    """Stored `José`, previously returned as `$5` followed by 7 mojibake bytes."""
    async def scenario():
        async with running_server() as port, connect(port) as client:
            await client.send(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nJos\xc3\xa9\r\n")
            assert await client.send(b"GET k\r\n") == b"$5\r\nJos\xc3\xa9\r\n"

    run(scenario())


def test_stream_stays_in_sync_after_a_non_ascii_reply():
    """The real damage of a wrong length: every later reply is misaligned."""
    async def scenario():
        async with running_server() as port, connect(port) as client:
            await client.send(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nJos\xc3\xa9\r\n")
            await client.send(b"GET k\r\n")
            assert await client.send(b"SET after value\r\n") == b"+OK\r\n"
            assert await client.send(b"GET after\r\n") == b"$5\r\nvalue\r\n"

    run(scenario())


def test_wrong_arity_error_has_a_single_code():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            reply = await client.send(b"GET\r\n")
            assert reply == b"-ERR wrong number of arguments for 'get' command\r\n"

    run(scenario())


def test_unknown_command_error():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            reply = await client.send(b"FLYAWAY\r\n")
            assert reply.startswith(b"-ERR Unknown command")
            assert not reply.startswith(b"-ERR ERR")

    run(scenario())


def test_wrongtype_error_is_reported():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            await client.send(b"LPUSH mylist a\r\n")
            reply = await client.send(b"GET mylist\r\n")
            assert reply.startswith(b"-WRONGTYPE ")

    run(scenario())


def test_del_alias_works():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            await client.send(b"SET k v\r\n")
            assert await client.send(b"DEL k\r\n") == b":1\r\n"
            assert await client.send(b"DEL k\r\n") == b":0\r\n"

    run(scenario())


def test_del_on_expired_key_reports_zero():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            await client.send(b"SET temp v 1\r\n")
            await asyncio.sleep(1.1)
            assert await client.send(b"DEL temp\r\n") == b":0\r\n"

    run(scenario())


def test_auth_is_rejected_when_no_password_is_configured():
    """Previously any AUTH returned +OK, which reads as 'authentication passed'."""
    async def scenario():
        async with running_server() as port, connect(port) as client:
            reply = await client.send(b"AUTH hunter2\r\n")
            assert reply.startswith(b"-ERR ")

    run(scenario())


def test_auth_required_and_enforced():
    async def scenario():
        async with running_server(password="s3cret") as port, connect(port) as client:
            assert (await client.send(b"GET k\r\n")).startswith(b"-NOAUTH ")
            assert (await client.send(b"AUTH wrong\r\n")).startswith(b"-WRONGPASS ")
            assert await client.send(b"AUTH s3cret\r\n") == b"+OK\r\n"
            assert await client.send(b"SET k v\r\n") == b"+OK\r\n"
            assert await client.send(b"GET k\r\n") == b"$1\r\nv\r\n"

    run(scenario())


def test_wrong_password_stays_unauthenticated():
    async def scenario():
        async with running_server(password="s3cret") as port, connect(port) as client:
            await client.send(b"AUTH wrong\r\n")
            assert (await client.send(b"GET k\r\n")).startswith(b"-NOAUTH ")

    run(scenario())


def test_pipelined_commands_are_all_answered():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            client.writer.write(b"SET a 1\r\nSET b 2\r\nGET a\r\n")
            await client.writer.drain()
            assert await read_reply(client.reader) == b"+OK\r\n"
            assert await read_reply(client.reader) == b"+OK\r\n"
            assert await read_reply(client.reader) == b"$1\r\n1\r\n"

    run(scenario())


def test_list_commands_over_the_wire():
    async def scenario():
        async with running_server() as port, connect(port) as client:
            assert await client.send(b"LPUSH mylist item1 item2\r\n") == b":2\r\n"
            reply = await client.send(b"LRANGE mylist 0 -1\r\n")
            assert reply == b"*2\r\n$5\r\nitem2\r\n$5\r\nitem1\r\n"

    run(scenario())
