import asyncio
import time
import json
import logging
from typing import Dict, Any, Tuple, Optional

# Configure logging to monitor the server's status.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StorageEngine:
    """
    The core storage engine for IgnisDB.
    Manages all data, expirations, and disk operations.
    Uses asyncio.Lock for concurrency safety.
    """
    def __init__(self, snapshot_path: str = 'ignisdb_snapshot.json'):
        # The main Python dictionary used for storing data.
        # Format: { key: (value, expiration_timestamp) }
        # If expiration_timestamp is None, the key never expires.
        self._data: Dict[str, Tuple[Any, Optional[float]]] = {}
        # A lock to prevent multiple coroutines from writing to the data structure simultaneously.
        self._lock = asyncio.Lock()
        # Path for the data persistence snapshot file.
        self._snapshot_path = snapshot_path
        logging.info("Storage engine initialized.")

    async def set(self, key: str, value: Any, expire_in_seconds: Optional[int] = None):
        """Sets a key-value pair. Optionally sets a time-to-live (TTL)."""
        async with self._lock:
            expiration_time = time.time() + expire_in_seconds if expire_in_seconds is not None else None
            self._data[key] = (value, expiration_time)
            logging.debug(f"SET: key='{key}', value='{value}', expire_at={expiration_time}")
        return "OK"

    async def get(self, key: str) -> Optional[Any]:
        """Gets the value of a key. Returns None if the key doesn't exist or has expired."""
        async with self._lock:
            item = self._data.get(key)
            if item is None:
                logging.debug(f"GET: key='{key}' not found.")
                return None

            value, expiration_time = item
            # Check if the key has expired
            if expiration_time is not None and time.time() > expiration_time:
                logging.info(f"GET: key='{key}' has expired. Deleting.")
                del self._data[key] # Lazy deletion of the expired key
                return None

            logging.debug(f"GET: key='{key}', value='{value}'")
            return value

    async def delete(self, key: str) -> int:
        """Deletes a key. Returns 1 if the key existed, 0 otherwise."""
        async with self._lock:
            if key in self._data:
                del self._data[key]
                logging.info(f"DELETE: key='{key}' deleted.")
                return 1
            logging.debug(f"DELETE: key='{key}' not found.")
            return 0
            
    async def expire(self, key: str, expire_in_seconds: int) -> int:
        """Sets a time-to-live on an existing key. Returns 1 if the key exists, 0 otherwise."""
        async with self._lock:
            item = self._data.get(key)
            if item is None:
                return 0

            value, _ = item
            expiration_time = time.time() + expire_in_seconds
            self._data[key] = (value, expiration_time)
            logging.info(f"EXPIRE: TTL for key='{key}' set to {expiration_time}.")
            return 1

    async def save_to_disk(self):
        """Saves the entire database (snapshot) to disk in JSON format."""
        async with self._lock:
            try:
                with open(self._snapshot_path, 'w') as f:
                    json.dump(self._data, f)
                logging.info(f"Database successfully saved to '{self._snapshot_path}'.")
            except Exception as e:
                logging.error(f"Error saving snapshot: {e}")

    async def load_from_disk(self):
        """Loads data from the snapshot file on disk."""
        try:
            with open(self._snapshot_path, 'r') as f:
                async with self._lock:
                    self._data = json.load(f)
                logging.info(f"Database successfully loaded from '{self._snapshot_path}'.")
        except FileNotFoundError:
            logging.warning(f"Snapshot file '{self._snapshot_path}' not found. Starting with an empty database.")
        except Exception as e:
            logging.error(f"Error loading snapshot: {e}")

class ProtocolParser:
    """
    Parses and processes raw text commands from the client.
    """
    def __init__(self, storage: StorageEngine):
        self._storage = storage

    async def process_command(self, command_raw: str) -> str:
        """Parses the incoming command, calls the relevant storage function, and formats the response."""
        parts = command_raw.strip().split()
        if not parts:
            return "-ERROR: Empty command\r\n"

        command = parts[0].upper()
        args = parts[1:]

        try:
            if command == "SET":
                if len(args) < 2: return "-ERROR: 'SET' command requires 'key' and 'value' arguments.\r\n"
                key, value = args[0], args[1]
                expire_seconds = None
                if len(args) == 4 and args[2].upper() == 'EX':
                    expire_seconds = int(args[3])
                
                result = await self._storage.set(key, value, expire_seconds)
                return f"+{result}\r\n"
            
            elif command == "GET":
                if len(args) != 1: return "-ERROR: 'GET' command requires 1 argument: 'key'\r\n"
                key = args[0]
                result = await self._storage.get(key)
                # Redis uses Bulk Strings for responses
                return f"${len(result)}\r\n{result}\r\n" if result is not None else "_(nil)\r\n"
            
            elif command == "DELETE":
                if len(args) != 1: return "-ERROR: 'DELETE' command requires 1 argument: 'key'\r\n"
                key = args[0]
                result = await self._storage.delete(key)
                # Redis uses Integers for responses
                return f":{result}\r\n"
            
            elif command == "EXPIRE":
                if len(args) != 2: return "-ERROR: 'EXPIRE' command requires 2 arguments: 'key' and 'seconds'\r\n"
                key, seconds = args[0], int(args[1])
                result = await self._storage.expire(key, seconds)
                return f":{result}\r\n"
            
            else:
                return f"-ERROR: Unknown command '{command}'\r\n"
        except (ValueError, IndexError) as e:
            return f"-ERROR: Invalid arguments for command: {e}\r\n"
        except Exception as e:
            logging.error(f"Unexpected error while processing command: {e}")
            return f"-ERROR: Server error\r\n"


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, parser: ProtocolParser):
    """The main coroutine that runs for each client connection."""
    addr = writer.get_extra_info('peername')
    logging.info(f"New connection from {addr}")
    
    try:
        while True:
            # Read up to 1024 bytes from the client
            data = await reader.read(1024)
            if not data:
                break # Connection closed by client

            command_raw = data.decode('utf-8')
            response = await parser.process_command(command_raw)
            
            writer.write(response.encode('utf-8'))
            await writer.drain() # Wait until the write buffer is empty

    except ConnectionResetError:
        logging.warning(f"Connection reset by peer: {addr}")
    except Exception as e:
        logging.error(f"Error handling client ({addr}): {e}")
    finally:
        logging.info(f"Connection closed for {addr}")
        writer.close()
        await writer.wait_closed()


async def periodic_snapshot(storage: StorageEngine, interval: int):
    """Periodically saves the database to disk."""
    while True:
        await asyncio.sleep(interval)
        logging.info(f"Starting periodic snapshot (interval: {interval} seconds)...")
        await storage.save_to_disk()

async def main():
    """The main server function. Starts the server and accepts connections."""
    host = '127.0.0.1'
    port = 6380
    snapshot_interval = 300 # Snapshot every 5 minutes

    storage = StorageEngine()
    await storage.load_from_disk()

    parser = ProtocolParser(storage)

    # Start the periodic snapshot task in the background
    asyncio.create_task(periodic_snapshot(storage, snapshot_interval))

    # Start the TCP server
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, parser),
        host,
        port
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    logging.info(f'IgnisDB server running on {addrs}...')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down the server...")
