import os
import json
import logging
import asyncio
import time
import base64
from typing import List, Tuple, Any, Dict, Optional

logger = logging.getLogger(__name__)

class AofHandler:
    """Manages writing commands to the AOF file for persistence."""
    def __init__(self, path: str, flush_interval: float = 1.0):
        self._path = path
        self._file = None
        self._buffer = []
        self._flush_interval = flush_interval
        self._running = False
        self._flush_task = None

    def open(self):
        """Opens the AOF file in append mode and starts background flusher."""
        try:
            self._file = open(self._path, 'a', buffering=1, encoding='utf-8')
            self._running = True
            loop = asyncio.get_event_loop()
            self._flush_task = loop.create_task(self._periodic_flush())
            logger.info(f"AOF file '{self._path}' opened for writing (Async Mode).")
        except Exception as e:
            logger.error(f"Failed to open AOF file: {e}")
            raise

    def write(self, command: str, *args: Any):
        """Buffers a command to be written to the AOF file."""
        if not self._running:
            return
        
        parts = [f"*{len(args) + 1}\r\n", f"${len(command)}\r\n{command}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        
        raw_command = "".join(parts)
        self._buffer.append(raw_command)

    async def _periodic_flush(self):
        """Background task to flush buffer to disk."""
        while self._running:
            await asyncio.sleep(self._flush_interval)
            await self.fsync()

    async def fsync(self):
        """Flushes the buffer to disk in a separate thread to avoid blocking."""
        if not self._buffer:
            return

        current_batch = list(self._buffer)
        self._buffer.clear()
        
        if not current_batch:
            return

        def _flush_to_disk():
            try:
                if self._file:
                    for cmd in current_batch:
                        self._file.write(cmd)
                    self._file.flush()
                    os.fsync(self._file.fileno())
            except Exception as e:
                logger.error(f"Error flushing AOF: {e}")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _flush_to_disk)

    def close(self):
        """Closes the AOF file and stops the flusher."""
        self._running = False
        if self._file:
            try:
                if self._buffer:
                    for cmd in self._buffer:
                        self._file.write(cmd)
                    self._file.flush()
                    os.fsync(self._file.fileno())
            except Exception as e:
                logger.error(f"Error executing final AOF flush: {e}")

            self._file.close()
            self._file = None
            logger.info("AOF file closed.")

    def load(self) -> List[Tuple[str, List[str]]]:
        """Reads the AOF file and parses commands to be replayed, with progress indicator."""
        commands = []
        if not os.path.exists(self._path):
            logger.warning(f"AOF file '{self._path}' not found. Starting empty.")
            return commands

        try:
            file_size = os.path.getsize(self._path)
            processed_bytes = 0
            last_progress_update = 0
            
            # Streaming read to avoid high RAM usage
            with open(self._path, 'r', encoding='utf-8') as f:
                logger.info(f"Loading AOF file ({file_size / (1024*1024):.2f} MB)...")
                
                while True:
                    # Parse RESP frame manually from stream
                    # We expect *<num_args>\r\n or empty lines
                    line = f.readline()
                    processed_bytes += len(line.encode('utf-8'))
                    
                    while line and not line.strip(): # Skip empty lines (headers)
                        line = f.readline()
                        processed_bytes += len(line.encode('utf-8'))
                    
                    if not line: break
                    
                    if not line.startswith('*'):
                        continue
                    
                    try:
                        num_args_str = line[1:].strip()
                        if not num_args_str: continue
                        num_args = int(num_args_str)
                        
                        args = []
                        for _ in range(num_args):
                            # Read $length
                            len_line = f.readline()
                            processed_bytes += len(len_line.encode('utf-8'))
                            
                            while len_line and not len_line.strip(): # Skip empty lines (length)
                                len_line = f.readline()
                                processed_bytes += len(len_line.encode('utf-8'))

                            if not len_line: break
                            
                            arg_len = int(len_line[1:].strip())
                            
                            # Read data
                            arg_line = f.readline() 
                            processed_bytes += len(arg_line.encode('utf-8'))
                            
                            # If we expect data (len > 0) but got empty line, it's separator artifact
                            # If len == 0, empty line is data
                            if arg_len > 0:
                                while arg_line and not arg_line.rstrip('\r\n'): 
                                    arg_line = f.readline()
                                    processed_bytes += len(arg_line.encode('utf-8'))
                            
                            if not arg_line: break
                            
                            args.append(arg_line.rstrip('\r\n'))
                        
                        if len(args) > 0:
                            commands.append((args[0], args[1:]))

                        # Progress Bar
                        if file_size > 0:
                            current_time = time.time()
                            if current_time - last_progress_update > 0.1: # Update every 100ms
                                progress = (processed_bytes / file_size) * 100
                                print(f"\rLoading AOF... {progress:.1f}% ({len(commands)} commands)", end='', flush=True)
                                last_progress_update = current_time

                    except (ValueError, IndexError) as e:
                        # Log debug only to avoid spam
                        logger.debug(f"Error parsing AOF line: {e}")
                        continue
                        
            print(f"\rLoading AOF... 100.0% ({len(commands)} commands) - Done.          ")
            logger.info(f"Loaded {len(commands)} commands from AOF.")
            
        except Exception as e:
            logger.error(f"Error loading AOF: {e}")
        
        return commands

    async def rewrite(self, data: Dict[str, Any]):
        """Rewrites the AOF file based on current memory state to reduce file size."""
        temp_path = self._path + ".rewrite"
        try:
            with open(temp_path, 'w', buffering=1, encoding='utf-8') as f:
                for key, value in data.items():
                    if not isinstance(value, tuple) or len(value) != 3:
                        continue
                        
                    data_type, content, expire_at = value
                    
                    if expire_at and time.time() > expire_at:
                        continue
                        
                    cmd = None
                    args = []
                    
                    if data_type == 'string':
                        cmd = "SET"
                        val_str = content
                        if isinstance(content, dict):
                            val_str = json.dumps(content, ensure_ascii=False)
                        args = [key, val_str]

                    elif data_type == 'list':
                        cmd = "LPUSH"
                        args = [key] + list(reversed(content))
                    
                    elif data_type == 'hash':
                        for field, val in content.items():
                            self._write_to_file(f, "HSET", key, field, val)
                        continue

                    elif data_type == 'set':
                        cmd = "SADD"
                        args = [key] + list(content)
                        
                    if cmd:
                        self._write_to_file(f, cmd, *args)
                        
                    if expire_at:
                        ttl = int(expire_at - time.time())
                        if ttl > 0:
                            self._write_to_file(f, "EXPIRE", key, ttl)

            was_running = self._running
            self._running = False
            
            if self._file:
                if self._buffer:
                    for c in self._buffer: self._file.write(c)
                self._file.close()
            
            if os.path.exists(self._path):
                os.remove(self._path)
            os.rename(temp_path, self._path)
            
            self.open()
            
            logger.info("AOF Rewrite complete. File compacted.")
            return True

        except Exception as e:
            logger.error(f"AOF Rewrite failed: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if not self._file and was_running:
                self.open()
            return False

    def _write_to_file(self, f, command, *args):
        """Helper to write RESP command to a file object"""
        parts = [f"*{len(args) + 1}\r\n", f"${len(command)}\r\n{command}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        f.write("".join(parts))

class SnapshotHandler:
    """Manages saving/loading database snapshots."""
    
    def __init__(self, path: str):
        self._path = path

    def save(self, data: Dict[str, Any]):
        """Saves data to a JSON snapshot file. Handles bytes via Base64."""
        
        def encode_value(val):
            if isinstance(val, bytes):
                return {"__type__": "bytes", "data": base64.b64encode(val).decode('ascii')}
            elif isinstance(val, list):
                return [encode_value(v) for v in val]
            elif isinstance(val, set):
                return [encode_value(v) for v in val]
            elif isinstance(val, dict):
                return {k: encode_value(v) for k, v in val.items()}
            elif isinstance(val, tuple):
                return [encode_value(v) for v in val]
            return val

        json_ready = {}
        for k, v in data.items():
            json_ready[k] = encode_value(v)

        try:
            with open(self._path, 'w', encoding='utf-8') as f:
                json.dump(json_ready, f)
            logger.info(f"Database snapshot saved to '{self._path}'.")
        except Exception as e:
            logger.error(f"Error saving snapshot: {e}")

    def load(self) -> Dict[str, Any]:
        """Loads data from a JSON snapshot file."""
        
        def decode_value(val):
            if isinstance(val, dict) and val.get("__type__") == "bytes":
                return base64.b64decode(val["data"])
            elif isinstance(val, list):
                return [decode_value(v) for v in val]
            elif isinstance(val, dict):
                return {k: decode_value(v) for k, v in val.items()}
            return val

        try:
            # Enforce UTF-8 encoding
            with open(self._path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            decoded_data = {}
            for k, v in raw_data.items():
                if isinstance(v, list) and len(v) == 3:
                    type_, val_, exp_ = v
                    decoded_val = decode_value(val_)
                    decoded_data[k] = (type_, decoded_val, exp_)
            
            logger.info(f"Database loaded from '{self._path}'.")
            return decoded_data
        except FileNotFoundError:
            logger.warning(f"Snapshot file '{self._path}' not found. Starting empty.")
            return {}
        except Exception as e:
            logger.error(f"Error loading snapshot: {e}")
            return {}

async def periodic_snapshot(storage, interval: int):
    """Background task to periodically save a snapshot."""
    while True:
        await asyncio.sleep(interval)
        logger.info("Starting periodic snapshot...")
        await storage.save_snapshot()
