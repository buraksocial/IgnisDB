import asyncio
import logging
import argparse
from typing import Optional
from .storage import StorageEngine
from .protocol import ProtocolHandler
from .mysql_protocol import MySQLProtocolHandler
from .persistence import AofHandler, SnapshotHandler, periodic_snapshot, iter_restore_commands
from .commands import CommandRegistry
from .commands_interface import ServerContext
from .exceptions import CommandError, WrongTypeError
from .pubsub import PubSubManager
from .security import SecurityManager

logger = logging.getLogger(__name__)

# Commands that mutate state: persisted to the AOF and propagated to replicas.
WRITE_COMMANDS = frozenset({
    'SET', 'DEL', 'DELETE', 'EXPIRE', 'LPUSH', 'HSET', 'SADD', 'SREM',
})


class IgnisServer:
    def __init__(self, host: str, port: int, persistence_mode: str, 
                 snapshot_path: str, aof_path: str, snapshot_interval: int, 
                 password: Optional[str] = None, encryption_key: Optional[str] = None):
        self.host = host
        self.port = port
        self.persistence_mode = persistence_mode
        self.snapshot_path = snapshot_path
        self.aof_path = aof_path
        self.snapshot_interval = snapshot_interval
        self.password = password
        
        # Components
        self.aof_handler = AofHandler(aof_path) if persistence_mode == 'aof' else None
        self.snapshot_handler = SnapshotHandler(snapshot_path)
        self.storage = StorageEngine()
        self.protocol = ProtocolHandler() 
        self.pubsub = PubSubManager()
        self.security = SecurityManager(encryption_key)
        self.replicas = set() # Set of writers (replicas)
        self._snapshot_task = None

        # Pre-instantiate commands for performance (Singleton-like usage)
        self.command_handlers = {}
        for cmd_name in CommandRegistry._commands:
            self.command_handlers[cmd_name] = CommandRegistry.get_command(cmd_name)()

    async def initialize(self):
        """Initializes persistence and loads data."""
        if self.aof_handler:
            self.aof_handler.open()
            
        if self.persistence_mode == 'snapshot':
            data = self.snapshot_handler.load()
            await self.storage.load_data(data)
            if self.snapshot_interval > 0:
                # Keep a reference: a bare create_task() may be garbage
                # collected mid-flight while the loop still holds only a weak
                # reference to it.
                self._snapshot_task = asyncio.create_task(
                    periodic_snapshot(self.storage, self.snapshot_handler, self.snapshot_interval)
                )
        elif self.persistence_mode == 'aof':
            logging.info("Replaying AOF...")
            commands = self.aof_handler.load()
            count = 0
            
            # Context for replay (no network writer)
            replay_context = ServerContext(storage=self.storage, pubsub=self.pubsub, server=self)
            
            for i, (cmd_name, args) in enumerate(commands):
                # FIX: Force uppercase lookup because keys in command_handlers are UPPERCASE
                handler = self.command_handlers.get(cmd_name.upper())
                
                if handler:
                    try:
                        await handler.execute(replay_context, *args)
                        count += 1
                    except Exception as e:
                        logger.error(f"Error replaying AOF command {cmd_name}: {e}")
            logging.info(f"Replayed {count} commands from AOF.")

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logger.info(f"New connection from {addr}")
        
        # Connection State
        authenticated = False if self.password else True
        
        # Localize lookups for hot loop
        parse_command = self.protocol.parse_command
        format_response = self.protocol.format_response
        command_handlers = self.command_handlers
        storage = self.storage
        aof = self.aof_handler
        write_cmds = WRITE_COMMANDS
        
        # Create Context for this connection
        conn_context = ServerContext(
            storage=storage, 
            pubsub=self.pubsub, 
            writer=writer, 
            aof=aof, 
            server=self,
            security=self.security
        )
        
        try:
            buffer = b""
            while True:
                data = await reader.read(65536) # Read up to 64KB
                if not data: break
                
                buffer += data
                
                while True:
                    frame, buffer = self.protocol.extract_frame(buffer)
                    if frame is None: break
                    
                    try:
                        cmd_name, args = parse_command(frame)
                        # Commands are case-insensitive, as in Redis: a client
                        # typing `set foo bar` must not get "Unknown command".
                        cmd_name = cmd_name.upper()

                        # Authentication Check
                        if self.password and not authenticated:
                            if cmd_name == 'AUTH':
                                if len(args) == 1 and args[0] == self.password:
                                    authenticated = True
                                    result = "OK"
                                else:
                                    raise CommandError("WRONGPASS invalid username-password pair or user is disabled.")
                            else:
                                raise CommandError("NOAUTH Authentication required.")
                        
                        else:
                            # Normal command execution
                            handler = command_handlers.get(cmd_name)
                            
                            if not handler:
                                raise CommandError(f"Unknown command '{cmd_name}'")
                            
                            result = await handler.execute(conn_context, *args)
                            
                            # Persist write commands
                            if cmd_name in write_cmds:
                                if aof:
                                    aof.write(cmd_name, *args)
                                
                                # Replication Propagate
                                self._propagate(cmd_name, args)
                            
                            # Special Admin Commands
                            if cmd_name == 'SYNC':
                                # Client wants to be a replica. Send the current
                                # dataset first, then register it for the live
                                # stream. There is no await between the two, so
                                # no write can slip in and be missed.
                                sent = self._send_full_resync(writer)
                                self.replicas.add(writer)
                                logger.info(f"Client {addr} registered as REPLICA ({sent} keys streamed)")
                                result = "OK"

                            elif cmd_name == 'REPLICAOF':
                                if len(args) != 2:
                                     raise CommandError("ERR wrong number of arguments for 'replicaof'")
                                host, port = args[0], args[1]
                                if host.upper() == "NO" and port.upper() == "ONE":
                                    # Turn off replication
                                    # TODO: implement stop replication
                                    result = "OK"
                                else:
                                    # Start replication task
                                    # connecting to master
                                    asyncio.create_task(self.connect_to_master(host, int(port)))
                                    result = "OK"

                            elif cmd_name == 'EXPORT':
                                # Export data to .ignis file
                                filename = "dump.ignis"
                                if len(args) > 0:
                                    filename = args[0]
                                    if not filename.endswith('.ignis'):
                                        filename += ".ignis"
                                
                                logger.info(f"Exporting data to {filename}...")
                                data_snapshot = await storage.get_all_data()
                                # Reuse SnapshotHandler logic for JSON dumping
                                exporter = SnapshotHandler(filename)
                                exporter.save(data_snapshot)
                                result = f"Data exported to {filename}"

                            elif cmd_name == 'IMPORT':
                                # Import data from .ignis file
                                filename = "dump.ignis"
                                if len(args) > 0:
                                    filename = args[0]
                                    if not filename.endswith('.ignis'):
                                        filename += ".ignis"
                                
                                import os
                                if not os.path.exists(filename):
                                    raise CommandError(f"ERR Import file '{filename}' not found")
                                
                                logger.info(f"Importing data from {filename}...")
                                importer = SnapshotHandler(filename)
                                loaded_data = importer.load()
                                
                                count = await storage.restore_data(loaded_data)
                                
                                # AOF Persistence for Imported Data
                                if aof:
                                    # Optimized: Instead of appending millions of commands to the AOF buffer (RAM spike),
                                    # we trigger a background rewrite which streams current DB state to disk efficiently.
                                    logger.info("Triggering background AOF rewrite to persist imported data...")
                                    asyncio.create_task(aof.rewrite(await storage.get_all_data()))
                                
                                result = f"Imported {count} keys from {filename}"

                            elif cmd_name == 'BGREWRITEAOF':
                                if aof:
                                    data_snapshot = await storage.get_all_data()
                                    logger.info("Starting AOF rewrite...")
                                    await aof.rewrite(data_snapshot)
                                    result = "Background append only file rewriting started" 
                                else:
                                    raise CommandError("AOF persistence is not enabled")

                            
                        response = format_response(result)
                        
                    except (CommandError, WrongTypeError, ValueError) as e:
                        response = format_response(e)
                    except Exception as e:
                        logger.error(f"Unexpected error: {e}")
                        response = format_response(CommandError("Server error"))
                    
                    writer.write(response)
                
                await writer.drain()
                
        except ConnectionResetError:
            logger.warning(f"Connection reset by {addr}")
        finally:
            self.pubsub.remove_client(writer)
            # A disconnected replica was never unregistered, so every later
            # write was propagated into a closed transport.
            self.replicas.discard(writer)
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection closed for {addr}")

    def _propagate(self, cmd_name: str, args) -> None:
        """Forwards a write command to every connected replica.

        Sent as RESP rather than an inline string: inline framing splits on
        whitespace, so `SET greeting "hello world"` would arrive at the replica
        as a three-argument SET and be rejected.
        """
        if not self.replicas:
            return

        try:
            payload = self.protocol.format_command_as_bytes(cmd_name, *args)
        except Exception:
            logger.exception("Error constructing replication payload for %s", cmd_name)
            return

        for replica in list(self.replicas):
            try:
                replica.write(payload)
            except Exception as e:
                logger.error(f"Error propagating to replica, dropping it: {e}")
                self.replicas.discard(replica)

    def _send_full_resync(self, writer) -> int:
        """Streams the current dataset to a freshly connected replica.

        Without this a replica that attaches to a non-empty master only ever
        sees writes made after it connected, and silently serves an incomplete
        dataset for the rest of its life.
        """
        sent = 0
        try:
            for command, args in iter_restore_commands(self.storage.snapshot()):
                writer.write(self.protocol.format_command_as_bytes(command, *args))
                sent += 1
        except Exception:
            logger.exception("Failed to stream full resync to replica")
        return sent

    async def _apply_from_master(self, frame: bytes) -> None:
        """Executes one command received from the master."""
        try:
            cmd_name, args = self.protocol.parse_command(frame)
        except Exception as e:
            logger.error(f"Replica could not parse frame from master: {e}")
            return

        cmd_name = cmd_name.upper()
        handler = self.command_handlers.get(cmd_name)
        if handler is None:
            # Status replies such as +OK are not commands; ignore them quietly.
            logger.debug("Replica ignoring non-command frame: %r", cmd_name)
            return

        try:
            ctx = ServerContext(
                storage=self.storage, pubsub=self.pubsub, server=self, aof=self.aof_handler
            )
            await handler.execute(ctx, *args)
            if self.aof_handler and cmd_name in WRITE_COMMANDS:
                self.aof_handler.write(cmd_name, *args)
        except Exception as e:
            logger.error(f"Replica execution error for {cmd_name}: {e}")

    async def connect_to_master(self, host: str, port: int):
        """Connects to a master instance and initiates replication."""
        try:
            logger.info(f"Connecting to MASTER {host}:{port}...")
            reader, writer = await asyncio.open_connection(host, port)

            # Note the real CRLF: this used to send the six literal characters
            # `SYNC\r\n` (escaped in the source), which the master's framer
            # never recognised as a complete command, so replication never
            # started at all.
            writer.write(self.protocol.format_command_as_bytes("SYNC"))
            await writer.drain()

            logger.info("Sent SYNC to master. Waiting for stream...")

            # The master streams RESP arrays, which contain CRLFs of their own.
            # Splitting the socket data on newlines cut commands in half, so
            # frames are extracted with the same framer the server uses.
            buffer = b""
            while True:
                data = await reader.read(65536)
                if not data: break

                buffer += data
                while True:
                    frame, buffer = self.protocol.extract_frame(buffer)
                    if frame is None: break
                    await self._apply_from_master(frame)

            logger.warning("Connection to MASTER closed.")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Failed to connect/sync with MASTER: {e}")

    async def handle_mysql_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logger.info(f"New MySQL connection from {addr}")
        handler = MySQLProtocolHandler(reader, writer, self.storage)
        await handler.handle_connection()

    async def start(self):
        await self.initialize()
        
        # Start Redis-like Server
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f"IgnisDB (Redis) server running on {self.host}:{self.port}")
        
        # Start MySQL-like Server (Port 3307 for now to avoid conflict)
        mysql_port = 3307
        try:
            mysql_server = await asyncio.start_server(self.handle_mysql_client, self.host, mysql_port)
            logger.info(f"IgnisDB (MySQL Compatibility) server running on {self.host}:{mysql_port}")
            asyncio.create_task(mysql_server.serve_forever())
        except Exception as e:
            logger.error(f"Failed to start MySQL listener: {e}")
        
        async with server:
            await server.serve_forever()

    def shutdown(self):
        """Flushes pending state to disk. Safe to call more than once."""
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            self._snapshot_task = None

        if self.persistence_mode == 'snapshot':
            # Without this, every write since the last interval tick is lost on
            # a clean shutdown.
            try:
                logger.info("Saving final snapshot before shutdown...")
                self.snapshot_handler.save(self.storage.snapshot())
            except Exception:
                logger.exception("Failed to save snapshot during shutdown.")

        if self.aof_handler:
            self.aof_handler.close()
