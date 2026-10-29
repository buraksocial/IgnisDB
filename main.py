"""IgnisDB v2 RESP server entry point."""
import argparse
import asyncio
import logging

from ignisdb import LSMEngine, Server


async def main() -> None:
    parser = argparse.ArgumentParser(description="IgnisDB v2 LSM database server")
    parser.add_argument("--data-dir", default="./data", help="Database data directory")
    parser.add_argument("--host", default="127.0.0.1", help="Address to bind")
    parser.add_argument("--port", type=int, default=6380, help="RESP port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    engine = LSMEngine(args.data_dir)
    await engine.open()
    server = Server(engine, host=args.host, port=args.port)
    try:
        await server.serve()
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
