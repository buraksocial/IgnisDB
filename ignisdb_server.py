import asyncio
import time
import json
import logging
from typing import Dict, Any, Tuple, Optional, List

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
        # Format: { key: (type, value, expiration_timestamp) }
        # type can be 'string', 'list', or 'hash'.
        # If expiration_timestamp is None, the key never expires.
        self._data: Dict[str, Tuple[str, Any, Optional[float]]] = {}
        # A lock to prevent multiple coroutines from writing to the data structure simultaneously.
        self._lock = asyncio.Lock()
        # Path for the data persistence snapshot file.
        self._snapshot_path = snapshot_path
        logging.info("Storage engine initialized.")

    async def _check_and_delete_expired(self, key: str) -> bool:
        """Internal helper to check for expiration and delete if needed. Returns True if expired."""
        item = self._data.get(key)
        if item:
            _, _, expiration_time = item
            if expiration_time is not None and time.time() > expiration_time:
                logging.info(f"Key '{key}' has expired. Deleting.")
                del self._data[key]
                return True
        return False

    async def set(self, key: str, value: Any, expire_in_seconds: Optional[int] = None):
        """Sets a key-value pair. This will always create a 'string' type."""
        async with self._lock:
            expiration_time = time.time() + expire_in_seconds if expire_in_seconds is not None else None
            # SET command always overwrites the key with a new string value.
            self._data[key] = ('string', value, expiration_time)
            logging.debug(f"SET: key='{key}', value='{value}', expire_at={expiration_time}")
        return "OK"

    async def get(self, key: str) -> Optional[Any]:
        """Gets the value of a key. Returns None if the key doesn't exist or has expired."""
        async with self._lock:
            if await self._check_and_delete_expired(key):
                return None
            
            item = self._data.get(key)
            if item is None:
                logging.debug(f"GET: key='{key}' not found.")
                return None

            type, value, _ = item
            # GET command only works for strings.
            if type != 'string':
                raise TypeError(f"Operation against a key holding the wrong kind of value")

            logging.debug(f"GET: key='{key}', value='{value}'")
            return value

    async def delete(self, key: str) -> int:
        """Deletes a key. Returns 1 if the key existed, 0 otherwise."""
        async with self._lock:
            if await self._check_and_delete_expired(key):
                return 1 # It existed before expiration check
            if key in self._data:
                del self._data[key]
                logging.info(f"DELETE: key='{key}' deleted.")
                return 1
            logging.debug(f"DELETE: key='{key}' not found.")
            return 0
            
    async def expire(self, key: str, expire_in_seconds: int) -> int:
        """Sets a time-to-live on an existing key. Returns 1 if the key exists, 0 otherwise."""
        async with self._lock:
            if await self._check_and_delete_expired(key):
                return 0 # Key has already expired and gone

            item = self._data.get(key)
            if item is None:
                return 0

            type, value, _ = item
            expiration_time = time.time() + expire_in_seconds
            self._data[key] = (type, value, expiration_time) # Preserve original type and value
            logging.info(f"EXPIRE: TTL for key='{key}' set to {expiration_time}.")
            return 1

    # --- New List Commands ---
    async def lpush(self, key: str, values: List[str]) -> int:
        """Prepends one or more values to a list. Returns the length of the list after the push."""
        async with self._lock:
            await self._check_and_delete_expired(key)
            item = self._data.get(key)
            
            if item is None:
                # Create new list if key doesn't exist
                new_list = list(reversed(values)) # LPUSH pushes one by one from left to right
                self._data[key] = ('list', new_list, None)
                return len(new_list)
            
            type, current_list, expiration = item
            if type != 'list':
                raise TypeError("Operation against a key holding the wrong kind of value")
            
            current_list[:0] = reversed(values)
            return len(current_list)

    async def lrange(self, key: str, start: int, stop: int) -> Optional[List[str]]:
        """Returns the specified elements of the list stored at key."""
        async with self._lock:
            if await self._check_and_delete_expired(key):
                return []
            
            item = self._data.get(key)
            if item is None:
                return []
            
            type, current_list, _ = item
            if type != 'list':
                raise TypeError("Operation against a key holding the wrong kind of value")
            
            # Redis LRANGE stop is inclusive
            if stop == -1:
                return current_list[start:]
            return current_list[start : stop + 1]

    # --- New Hash Commands ---
    async def hset(self, key: str, field: str, value: str) -> int:
        """Sets the string value of a hash field. Returns 1 if field is a new field, 0 if it was updated."""
        async with self._lock:
            await self._check_and_delete_expired(key)
            item = self._data.get(key)

            if item is None:
                # Create new hash if key doesn't exist
                new_hash = {field: value}
                self._data[key] = ('hash', new_hash, None)
                return 1
            
            type, current_hash, expiration = item
            if type != 'hash':
                raise TypeError("Operation against a key holding the wrong kind of value")
            
            is_new_field = 1 if field not in current_hash else 0
            current_hash[field] = value
            return is_new_field

    async def hget(self, key: str, field: str) -> Optional[str]:
        """Gets the value of a hash field."""
        async with self._lock:
            if await self._check_and_delete_expired(key):
                return None
            
            item = self._data.get(key)
            if item is None:
                return None

            type, current_hash, _ = item
            if type != 'hash':
                raise TypeError("Operation against a key holding the wrong kind of value")
            
            return current_hash.get(field)

    async def save_to_disk(self):
        """Saves the entire database (snapshot) to disk in JSON format."""
        async with self._lock:
            # Clean expired keys before saving
            keys_to_delete = [key for key, (_, _, exp) in self._data.items() if exp is not None and time.time() > exp]
            for key in keys_to_delete:
                del self._data[key]

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
            # --- String Commands ---
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
                return f"${len(result)}\r\n{result}\r\n" if result is not None else "_(nil)\r\n"
            
            # --- Generic Commands ---
            elif command == "DELETE":
                if len(args) != 1: return "-ERROR: 'DELETE' command requires 1 argument: 'key'\r\n"
                key = args[0]
                result = await self._storage.delete(key)
                return f":{result}\r\n"
            
            elif command == "EXPIRE":
                if len(args) != 2: return "-ERROR: 'EXPIRE' command requires 2 arguments: 'key' and 'seconds'\r\n"
                key, seconds = args[0], int(args[1])
                result = await self._storage.expire(key, seconds)
                return f":{result}\r\n"

            # --- List Commands ---
            elif command == "LPUSH":
                if len(args) < 2: return "-ERROR: 'LPUSH' requires at least a key and one value\r\n"
                key, values = args[0], args[1:]
                result = await self._storage.lpush(key, values)
                return f":{result}\r\n"

            elif command == "LRANGE":
                if len(args) != 3: return "-ERROR: 'LRANGE' requires key, start, and stop arguments\r\n"
                key, start, stop = args[0], int(args[1]), int(args[2])
                result = await self._storage.lrange(key, start, stop)
                # Format as RESP Array
                if result is None: return "_(nil)\r\n"
                response_parts = [f"*{len(result)}\r\n"]
                for item in result:
                    response_parts.append(f"${len(item)}\r\n{item}\r\n")
                return "".join(response_parts)

            # --- Hash Commands ---
            elif command == "HSET":
                if len(args) != 3: return "-ERROR: 'HSET' requires key, field, and value arguments\r\n"
                key, field, value = args[0], args[1], args[2]
                result = await self._storage.hset(key, field, value)
                return f":{result}\r\n"
            
            elif command == "HGET":
                if len(args) != 2: return "-ERROR: 'HGET' requires key and field arguments\r\n"
                key, field = args[0], args[1]
                result = await self._storage.hget(key, field)
                return f"${len(result)}\r\n{result}\r\n" if result is not None else "_(nil)\r\n"

            else:
                return f"-ERROR: Unknown command '{command}'\r\n"
        
        except TypeError as e:
            return f"-WRONGTYPE {e}\r\n"
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
            data = await reader.read(1024)
            if not data:
                break 

            command_raw = data.decode('utf-8')
            response = await parser.process_command(command_raw)
            
            writer.write(response.encode('utf-8'))
            await writer.drain()

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
    snapshot_interval = 300 

    storage = StorageEngine()
    await storage.load_from_disk()

    parser = ProtocolParser(storage)

    asyncio.create_task(periodic_snapshot(storage, snapshot_interval))

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
