import asyncio
import time
import json
import logging
import argparse
from typing import Dict, Any, Tuple, Optional, List

# --- Logging and Error Classes ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CommandError(Exception):
    """Custom exception for command-related errors."""
    pass

class WrongTypeError(TypeError):
    """Custom exception for data type mismatches."""
    pass


# --- AOF (Append-Only File) Handler ---
class AofHandler:
    """Manages writing commands to the AOF file for persistence."""
    def __init__(self, path: str):
        self._path = path
        self._file = None

    def open(self):
        """Opens the AOF file in append mode."""
        # Open with line buffering (1) to ensure writes are flushed promptly.
        self._file = open(self._path, 'a', buffering=1)
        logging.info(f"AOF file '{self._path}' opened for writing.")

    def write(self, command: str, *args: Any):
        """Writes a command to the AOF file in RESP Array format."""
        if not self._file:
            return
        
        # Construct the command in RESP (Redis Serialization Protocol) format.
        parts = [f"*{len(args) + 1}\r\n", f"${len(command)}\r\n{command}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        
        self._file.write("".join(parts))
        logging.debug(f"Wrote to AOF: {command} {' '.join(map(str, args))}")

    def close(self):
        """Closes the AOF file."""
        if self._file:
            self._file.close()
            self._file = None
            logging.info("AOF file closed.")


# --- Core Storage Engine ---
class StorageEngine:
    """
    Manages all data, expirations, and persistence operations.
    Concurrency is handled via an asyncio.Lock.
    """
    def __init__(self, snapshot_path: str, aof_handler: Optional[AofHandler]):
        # Data format: { key: (type, value, expiration_timestamp) }
        self._data: Dict[str, Tuple[str, Any, Optional[float]]] = {}
        self._lock = asyncio.Lock()
        self._snapshot_path = snapshot_path
        self._aof = aof_handler

    async def _check_and_delete_expired_unlocked(self, key: str) -> bool:
        """
        Internal helper to check for key expiration and delete if needed.
        This version does not acquire the lock, assuming it's already held.
        """
        item = self._data.get(key)
        if item:
            _, _, expiration_time = item
            if expiration_time is not None and time.time() > expiration_time:
                logging.info(f"Key '{key}' has expired. Deleting.")
                del self._data[key]
                return True
        return False

    async def execute_command(self, command: str, args: List[Any], propagate_func=None):
        """
        Central dispatcher for executing a single command.
        It acquires the lock and calls the appropriate internal method.
        """
        command_upper = command.upper()
        method_name = f"_exec_{command_upper}"
        if not hasattr(self, method_name):
            raise CommandError(f"Unknown command '{command}'")
        
        method = getattr(self, method_name)
        
        async with self._lock:
            # Expired keys are handled lazily within each command implementation.
            result = await method(*args)
            
            is_write_command = command_upper in ["SET", "DELETE", "EXPIRE", "LPUSH", "HSET"]
            if is_write_command:
                # If the command was a write, persist it.
                if self._aof:
                    self._aof.write(command, *args)
                # And propagate it to replicas.
                if propagate_func:
                    await propagate_func(command, *args)
            return result

    async def execute_transaction(self, command_queue: List[Tuple[str, List[Any]]], propagate_func=None):
        """
        Executes a list of commands from a MULTI/EXEC block atomically.
        """
        results = []
        write_commands_in_txn = []

        async with self._lock:
            # All commands in a transaction are executed under a single lock.
            for command, args in command_queue:
                command_upper = command.upper()
                method_name = f"_exec_{command_upper}"
                if not hasattr(self, method_name):
                    raise CommandError(f"Unknown command '{command}' in transaction")
                
                method = getattr(self, method_name)
                
                try:
                    result = await method(*args)
                    results.append(result)
                    
                    is_write_command = command_upper in ["SET", "DELETE", "EXPIRE", "LPUSH", "HSET"]
                    if is_write_command:
                        write_commands_in_txn.append((command, args))

                except Exception as e:
                    # If any command fails, the entire transaction is aborted. No changes are kept.
                    logging.warning(f"Transaction failed on command '{command}': {e}. Rolling back.")
                    raise CommandError(f"Transaction aborted: {e}")

            # If the entire transaction succeeded, persist and propagate the write commands.
            for cmd, ags in write_commands_in_txn:
                if self._aof:
                    self._aof.write(cmd, *ags)
                if propagate_func:
                    await propagate_func(cmd, *ags)
        
        return results

    # --- LOCKED Command Implementations (called by the dispatcher) ---
    
    async def _exec_GET(self, key: str):
        if await self._check_and_delete_expired_unlocked(key): return None
        item = self._data.get(key)
        if item is None: return None
        type, value, _ = item
        if type != 'string': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        return value

    async def _exec_SET(self, key: str, value: Any, expire_in_seconds: Optional[str] = None):
        expiration_time = None
        if expire_in_seconds is not None:
            expiration_time = time.time() + int(expire_in_seconds)
        self._data[key] = ('string', value, expiration_time)
        return "OK"

    async def _exec_DELETE(self, key: str):
        if await self._check_and_delete_expired_unlocked(key): return 1
        if key in self._data:
            del self._data[key]
            return 1
        return 0

    async def _exec_EXPIRE(self, key: str, seconds: str):
        if await self._check_and_delete_expired_unlocked(key) or key not in self._data: return 0
        type, value, _ = self._data[key]
        self._data[key] = (type, value, time.time() + int(seconds))
        return 1
    
    async def _exec_LPUSH(self, key: str, *values: List[str]):
        await self._check_and_delete_expired_unlocked(key)
        item = self._data.get(key)
        if item is None:
            new_list = list(reversed(values))
            self._data[key] = ('list', new_list, None)
            return len(new_list)
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        current_list[:0] = reversed(values)
        return len(current_list)

    async def _exec_LRANGE(self, key: str, start: str, stop: str):
        start_idx, stop_idx = int(start), int(stop)
        if await self._check_and_delete_expired_unlocked(key): return []
        item = self._data.get(key)
        if item is None: return []
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        if stop_idx == -1: return current_list[start_idx:]
        return current_list[start_idx : stop_idx + 1]

    async def _exec_HSET(self, key: str, field: str, value: str):
        await self._check_and_delete_expired_unlocked(key)
        item = self._data.get(key)
        if item is None:
            self._data[key] = ('hash', {field: value}, None)
            return 1
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        is_new = 1 if field not in current_hash else 0
        current_hash[field] = value
        return is_new

    async def _exec_HGET(self, key: str, field: str):
        if await self._check_and_delete_expired_unlocked(key): return None
        item = self._data.get(key)
        if item is None: return None
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        return current_hash.get(field)

    # --- Persistence and Replication Helpers ---

    async def get_all_data_as_commands(self) -> List[Tuple[str, List[Any]]]:
        """Returns a list of commands to reconstruct the current dataset (for replication sync)."""
        commands = []
        async with self._lock:
            for key, (type, value, exp) in self._data.items():
                if await self._check_and_delete_expired_unlocked(key):
                    continue
                if type == 'string':
                    commands.append(("SET", [key, value]))
                elif type == 'list':
                    if value: commands.append(("LPUSH", [key] + list(reversed(value))))
                elif type == 'hash':
                    for field, f_value in value.items():
                        commands.append(("HSET", [key, field, f_value]))
                
                if exp:
                    ttl = int(exp - time.time())
                    if ttl > 0:
                        commands.append(("EXPIRE", [key, ttl]))
        return commands

    async def save_snapshot(self):
        """Saves the entire database to a JSON snapshot file."""
        async with self._lock:
            logging.info("Cleaning expired keys before snapshotting...")
            # Create a list of keys to avoid iterating over a changing dictionary
            all_keys = list(self._data.keys())
            for key in all_keys:
                await self._check_and_delete_expired_unlocked(key)
            try:
                with open(self._snapshot_path, 'w') as f:
                    json.dump(self._data, f)
                logging.info(f"Database snapshot successfully saved to '{self._snapshot_path}'.")
            except Exception as e:
                logging.error(f"Error saving snapshot: {e}")

    async def load_from_snapshot(self):
        """Loads the database from a JSON snapshot file."""
        try:
            with open(self._snapshot_path, 'r') as f:
                async with self._lock:
                    self._data = json.load(f)
                logging.info(f"Database successfully loaded from '{self._snapshot_path}'.")
        except FileNotFoundError:
            logging.warning(f"Snapshot file '{self._snapshot_path}' not found. Starting with an empty database.")
        except Exception as e:
            logging.error(f"Error loading snapshot: {e}")


# --- Protocol and Replication Handling ---
class ProtocolHandler:
    """Parses raw client data into commands and formats responses into RESP."""
    
    def parse_command(self, command_raw: str) -> Tuple[str, List[str]]:
        """Parses a raw string command into a (command, [args]) tuple."""
        parts = command_raw.strip().split()
        if not parts:
            raise CommandError("Empty command")
        return parts[0], parts[1:]

    def format_response(self, result: Any) -> str:
        """Formats a Python object into a RESP string for the client."""
        if result is None:
            return "_(nil)\r\n"
        elif isinstance(result, str):
            if result == "OK" or result == "QUEUED":
                return f"+{result}\r\n"
            return f"${len(result)}\r\n{result}\r\n"
        elif isinstance(result, int):
            return f":{result}\r\n"
        elif isinstance(result, list):
            # Recursively format items in a list (for LRANGE, EXEC)
            response_parts = [f"*{len(result)}\r\n"]
            for item in result:
                response_parts.append(self.format_response(item))
            return "".join(response_parts)
        elif isinstance(result, Exception):
            err_type = "WRONGTYPE" if isinstance(result, WrongTypeError) else "ERR"
            return f"-{err_type} {str(result)}\r\n"
        else:
            logging.error(f"Cannot format unknown response type: {type(result)}")
            return f"-ERR Server error: cannot format response\r\n"

    def format_command_as_bytes(self, command: str, *args: Any) -> bytes:
        """Formats a command and arguments into a RESP byte string (for replication)."""
        parts = [f"*{len(args) + 1}\r\n", f"${len(command)}\r\n{command}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        return "".join(parts).encode('utf-8')


class ReplicationManager:
    """Manages master-slave replication logic."""
    def __init__(self, role: str, storage: StorageEngine, protocol: ProtocolHandler, config: argparse.Namespace):
        self.role = role
        self.storage = storage
        self.protocol = protocol
        self.config = config
        self.slaves: List[asyncio.StreamWriter] = []

    async def add_slave(self, writer: asyncio.StreamWriter):
        """Adds a new replica and begins a full synchronization."""
        peername = writer.get_extra_info('peername')
        logging.info(f"New replica connection from {peername}. Starting full sync.")
        
        # Perform the full sync by sending all current data as commands.
        all_commands = await self.storage.get_all_data_as_commands()
        logging.info(f"Sending {len(all_commands)} commands to replica {peername} for initial sync.")
        for command, args in all_commands:
            writer.write(self.protocol.format_command_as_bytes(command, *args))
        
        await writer.drain()
        self.slaves.append(writer)
        logging.info(f"Full sync for replica {peername} completed. Now in live propagation mode.")

    async def propagate(self, command: str, *args: Any):
        """Propagates a write command to all connected replicas."""
        if not self.slaves:
            return
        
        logging.debug(f"Propagating command to {len(self.slaves)} replicas: {command} {args}")
        formatted_command = self.protocol.format_command_as_bytes(command, *args)
        
        disconnected_slaves = []
        for slave_writer in self.slaves:
            if slave_writer.is_closing():
                disconnected_slaves.append(slave_writer)
                continue
            try:
                slave_writer.write(formatted_command)
                await slave_writer.drain()
            except ConnectionError:
                logging.warning(f"Connection error writing to replica: {slave_writer.get_extra_info('peername')}. Removing from list.")
                disconnected_slaves.append(slave_writer)
        
        # Clean up list of replicas that have disconnected.
        for dw in disconnected_slaves:
            if dw in self.slaves:
                self.slaves.remove(dw)

    async def connect_to_master(self):
        """Task for a replica to connect to its master and listen for commands."""
        while True:
            try:
                logging.info(f"Connecting to master at {self.config.master_host}:{self.config.master_port}")
                reader, writer = await asyncio.open_connection(self.config.master_host, self.config.master_port)
                
                # Identify itself to the master as a replica.
                writer.write(self.protocol.format_command_as_bytes("REPLICAOF", "listening-port", self.config.port))
                await writer.drain()
                logging.info("Successfully connected to master, awaiting commands.")

                # In a real RESP parser, this would be a loop that reads and parses streams.
                # For simplicity, we assume commands arrive as distinct chunks.
                while True:
                    data = await reader.read(4096)
                    if not data:
                        logging.warning("Connection to master lost.")
                        break
                    
                    try:
                        # This simple parsing assumes one command per read, which is not robust.
                        # A proper implementation would use a streaming parser.
                        command, args = self.protocol.parse_command(data.decode('utf-8'))
                        # Apply commands from master directly, without taking a lock.
                        # Replicas do not write to AOF or propagate further.
                        await self.storage.execute_command(command, args, propagate_func=None)
                        logging.debug(f"Applied command from master: {command}")
                    except Exception as e:
                        logging.error(f"Error processing command from master: {e}")

            except ConnectionRefusedError:
                logging.error("Master refused connection. Retrying in 5 seconds.")
            except Exception as e:
                logging.error(f"Error connecting to master: {e}. Retrying in 5 seconds.")
            
            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(5)


# --- Server and Client Handling ---

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, storage: StorageEngine, protocol: ProtocolHandler, repl_manager: ReplicationManager):
    """Main coroutine to handle a single client connection."""
    addr = writer.get_extra_info('peername')
    logging.info(f"New connection from {addr} (Role: {repl_manager.role})")
    
    in_transaction = False
    command_queue = []

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break 

            command_raw = data.decode('utf-8')
            response = ""

            try:
                command, args = protocol.parse_command(command_raw)

                # --- Replication-specific Commands ---
                if command == 'REPLICAOF' and repl_manager.role == 'master':
                    # This connection is now a replica. Hand it off to the replication manager.
                    # It will no longer be treated as a regular client.
                    await repl_manager.add_slave(writer)
                    await writer.wait_closed() # Keep connection open for master-to-slave propagation.
                    break 

                # --- Write Command Blocking on Replicas ---
                is_write_command = command in ["SET", "DELETE", "EXPIRE", "LPUSH", "HSET", "MULTI"]
                if repl_manager.role == 'slave' and is_write_command:
                    raise CommandError("READONLY You can't write against a read-only replica.")

                # --- Transaction Commands ---
                if command == 'MULTI':
                    if in_transaction: raise CommandError("MULTI calls cannot be nested")
                    in_transaction = True
                    command_queue = []
                    response = protocol.format_response("OK")
                
                elif command == 'DISCARD':
                    if not in_transaction: raise CommandError("DISCARD without MULTI")
                    in_transaction = False
                    command_queue = []
                    response = protocol.format_response("OK")

                elif command == 'EXEC':
                    if not in_transaction: raise CommandError("EXEC without MULTI")
                    results = await storage.execute_transaction(command_queue, propagate_func=repl_manager.propagate)
                    in_transaction = False
                    command_queue = []
                    response = protocol.format_response(results)

                elif in_transaction:
                    command_queue.append((command, args))
                    response = protocol.format_response("QUEUED")
                
                else: # --- Standard Command Execution ---
                    result = await storage.execute_command(command, args, propagate_func=repl_manager.propagate)
                    response = protocol.format_response(result)

            except (CommandError, WrongTypeError, ValueError) as e:
                response = protocol.format_response(e)
            except Exception as e:
                logging.error(f"Unexpected error while handling client ({addr}): {e}")
                response = protocol.format_response(CommandError("Server error"))

            writer.write(response.encode('utf-8'))
            await writer.drain()

    except ConnectionResetError:
        logging.warning(f"Connection reset by peer: {addr}")
    finally:
        logging.info(f"Connection closed for {addr}")
        writer.close()
        await writer.wait_closed()


async def periodic_snapshot(storage: StorageEngine, interval: int):
    """Background task to periodically save a snapshot of the database."""
    while True:
        await asyncio.sleep(interval)
        logging.info("Starting periodic snapshot...")
        await storage.save_snapshot()


async def main(args):
    """The main entry point for the IgnisDB server."""
    host = '127.0.0.1'
    args.port = args.port if args.port else (6380 if args.role == 'master' else 6381)

    # Initialize persistence, storage, protocol, and replication handlers
    aof_handler = AofHandler(args.aof_file) if args.persistence_mode == 'aof' else None
    if aof_handler: aof_handler.open()
    
    storage = StorageEngine(snapshot_path=args.snapshot_file, aof_handler=aof_handler)
    protocol = ProtocolHandler()
    repl_manager = ReplicationManager(args.role, storage, protocol, args)

    # Load data from disk based on persistence mode
    if args.persistence_mode == 'snapshot':
        await storage.load_from_snapshot()
    # Note: AOF loading is not implemented. A robust implementation would require
    # replaying the AOF file on startup.
    
    # Start background tasks
    if args.persistence_mode == 'snapshot' and args.role == 'master':
        asyncio.create_task(periodic_snapshot(storage, args.snapshot_interval))
    
    if args.role == 'slave':
        asyncio.create_task(repl_manager.connect_to_master())
    
    # Start the main TCP server
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, storage, protocol, repl_manager),
        host,
        args.port
    )

    logging.info(f'IgnisDB server running on {host}:{args.port} | Role: {args.role.upper()} | Persistence: {args.persistence_mode}')

    try:
        async with server:
            await server.serve_forever()
    finally:
        if aof_handler:
            aof_handler.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IgnisDB - An in-memory Key-Value Database with Python asyncio.")
    
    # Server Role and Networking
    parser.add_argument('--role', type=str, choices=['master', 'slave'], default='master', help="The role of this server instance.")
    parser.add_argument('--port', type=int, help="Port to listen on. Defaults to 6380 for master, 6381 for slave.")
    parser.add_argument('--master-host', type=str, default='127.0.0.1', help="Hostname of the master (used by slaves).")
    parser.add_argument('--master-port', type=int, default=6380, help="Port of the master (used by slaves).")
    
    # Persistence
    parser.add_argument('--persistence-mode', type=str, choices=['snapshot', 'aof'], default='snapshot', help="The persistence mode.")
    parser.add_argument('--snapshot-file', type=str, default='ignisdb_snapshot.json', help="Path for the snapshot file.")
    parser.add_argument('--aof-file', type=str, default='ignisdb.aof', help="Path for the Append-Only File.")
    parser.add_argument('--snapshot-interval', type=int, default=300, help="Interval in seconds for periodic snapshotting.")
    
    cli_args = parser.parse_args()
    
    try:
        asyncio.run(main(cli_args))
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
