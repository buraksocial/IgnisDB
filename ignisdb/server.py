import asyncio
import logging
import argparse
from typing import Optional
from .storage import StorageEngine
from .protocol import ProtocolHandler
from .mysql_protocol import MySQLProtocolHandler
from .persistence import AofHandler, SnapshotHandler, periodic_snapshot
from .commands import CommandRegistry
from .commands_interface import ServerContext
from .exceptions import CommandError, WrongTypeError
from .pubsub import PubSubManager
from .security import SecurityManager

logger = logging.getLogger(__name__)

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
                asyncio.create_task(periodic_snapshot(self.storage, self.snapshot_interval))
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
        write_cmds = {'SET', 'DELETE', 'EXPIRE', 'LPUSH', 'HSET', 'SADD', 'SREM'}
        
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
                        
                        # Authentication Check
                        if self.password and not authenticated:
                            if cmd_name.upper() == 'AUTH':
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
                                if self.replicas:
                                    try:
                                        # Use Inline format because ProtocolHandler only supports Inline parsing currently.
                                        # format: CMD arg1 arg2\r\n
                                        # Note: This is not binary safe for args with spaces, but consistent with current parser.
                                        
                                        # Convert args to string
                                        str_args = [str(arg) for arg in args]
                                        inline_cmd = f"{cmd_name} {' '.join(str_args)}\\r\\n".encode('utf-8')
                                        
                                        for replica in list(self.replicas):
                                            try:
                                                replica.write(inline_cmd)
                                            except Exception as e:
                                                logger.error(f"Error persisting to replica: {e}")
                                                self.replicas.discard(replica)
                                    except Exception as e:
                                        logger.error(f"Error constructing replication payload: {e}")
                            
                            # Special Admin Commands
                            if cmd_name.upper() == 'SYNC':
                                # Client wants to be a replica
                                self.replicas.add(writer)
                                logger.info(f"Client {addr} registered as REPLICA")
                                # Ideally we send a snapshot here.
                                # For MVP, we just start forwarding new writes.
                                # But we should send standard "OK" or "+FULLRESYNC"
                                # Redis sends RDB. We'll send OK to keep connection alive.
                                # IMPORTANT: Replica usually does NOT expect standard response to SYNC, 
                                # it expects RDB stream.
                                # Let's send a simple OK so replica knows we accepted.
                                pass # We don't return 'result' here if we want to stream?
                                # Actually, let's just return OK and let logic flow.
                                result = "OK"

                            elif cmd_name.upper() == 'REPLICAOF':
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

                            elif cmd_name.upper() == 'EXPORT':
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

                            elif cmd_name.upper() == 'IMPORT':
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

                            elif cmd_name.upper() == 'BGREWRITEAOF':
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
                    
                    writer.write(response.encode('utf-8'))
                
                await writer.drain()
                
        except ConnectionResetError:
            logger.warning(f"Connection reset by {addr}")
        finally:
            self.pubsub.remove_client(writer)
            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection closed for {addr}")

    async def connect_to_master(self, host: str, port: int):
        """Connects to a master instance and initiates replication."""
        try:
            logger.info(f"Connecting to MASTER {host}:{port}...")
            reader, writer = await asyncio.open_connection(host, port)
            
            # 1. PING (optional, but good)
            # 2. AUTH (if needed)
            # 3. SYNC
            writer.write(b"SYNC\\r\\n")
            await writer.drain()
            
            logger.info("Sent SYNC to master. Waiting for stream...")
            
            # Loop to read commands from Master and execute them locally
            while True:
                data = await reader.read(65536)
                if not data: break
                
                # We need to process these commands as if they were local writes
                # But without writing to AOF? Or YES writing to AOF?
                # Usually Replicas DO write to their own AOF.
                # We can reuse handle_client logic but we don't have a 'writer' for response?
                # Master doesn't expect responses to propagated commands.
                # So we need a special 'execute_command' function that doesn't reply.
                
                # Quick hack: Parse and execute directly
                buffer = data
                while b'\\n' in buffer:
                    line, buffer = buffer.split(b'\\n', 1)
                    line = line.strip()
                    if not line: continue
                     
                    # If line starts with *, it's RESP array.
                    # Our current parser might be simple.
                    # As defined in replica propagation above, we send RESP.
                    # If our ProtocolHandler parses RESP, use it.
                    # For now, let's assume we can parse it.
                    # NOTE: This simple parser in handle_client splits by \\n.
                    # RESP arrays have newlines.
                    # This indicates we need a proper RESP parser for replication to work 100%.
                    # But for now, let's try to reuse what we have.
                    
                    try:
                        cmd_name, args = self.protocol.parse_command(line)
                        if cmd_name:
                             handler = self.command_handlers.get(cmd_name)
                             if handler:
                                 # Create a dummy context
                                 # We pass 'writer=None' so commands don't fail if they don't need it.
                                 # PubSub commands need it, but we unlikely replicate subscribe.
                                 ctx = ServerContext(storage=self.storage, pubsub=self.pubsub, server=self, aof=self.aof_handler)
                                 await handler.execute(ctx, *args)
                                 # We also write to our AOF if enabled
                                 if self.aof_handler and cmd_name in {'SET', 'DELETE', 'SADD', 'SREM'}:
                                     self.aof_handler.write(cmd_name, *args)
                    except Exception as e:
                        logger.error(f"Replica execution error: {e}")

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
        if self.aof_handler:
            self.aof_handler.close()
