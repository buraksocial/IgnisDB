import asyncio

async def main():
    r, w = await asyncio.open_connection('127.0.0.1', 6381)
    w.write(b"EXPORT snapshot_test.json\r\n")
    await w.drain()
    resp = await r.read(4096)
    print(resp.decode(errors='replace').strip())
    w.close()
    await w.wait_closed()

asyncio.run(main())
