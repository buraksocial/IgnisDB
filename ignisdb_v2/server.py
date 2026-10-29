"""
RESP Protocol Server - Backward compatible with IgnisDB v1 clients

Wraps LSMEngine with TCP server supporting RESP protocol.
All v1 commands work unchanged; new v2 commands added.
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Any, List

from .engine import LSMEngine

logger = logging.getLogger(__name__)


class Server:
    """
    TCP server with RESP protocol support.
    
    Backward compatible with IgnisDB v1 clients.
    Supports both inline and RESP array command formats.
    """
    
    def __init__(self, engine: LSMEngine, host: str = 'localhost', port: int = 6380):
        self.engine = engine
        self.host = host
        self.port = port
        self._server = None
        self._running = False
        
        # In-memory data structures for backward compatibility
        # These wrap the LSM engine for complex types
        self._lists: Dict[bytes, List[bytes]] = {}
        self._hashes: Dict[bytes, Dict[bytes, bytes]] = {}
        self._sets: Dict[bytes, set] = {}
        self._ttls: Dict[bytes, float] = {}  # key -> expiry timestamp
    
    async def serve(self):
        """Start the server."""
        self._running = True
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        logger.info(f"IgnisDB v2 server running on {self.host}:{self.port}")
        
        async with self._server:
            await self._server.serve_forever()
    
    async def stop(self):
        """Stop the server."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self.engine.close()
    
    async def _handle_client(self, reader: asyncio.StreamReader, 
                              writer: asyncio.StreamWriter):
        """Handle a single client connection."""
        addr = writer.get_extra_info('peername')
        logger.debug(f"New connection from {addr}")
        
        buffer = b""
        try:
            while self._running:
                data = await reader.read(65536)
                if not data:
                    break
                
                buffer += data
                
                while True:
                    frame, buffer = self._extract_frame(buffer)
                    if frame is None:
                        break
                    
                    try:
                        cmd, args = self._parse_command(frame)
                        result = await self._execute_command(cmd, args)
                        response = self._format_response(result)
                        writer.write(response.encode('utf-8'))
                    except Exception as e:
                        error_resp = f"-ERR {str(e)}\r\n"
                        writer.write(error_resp.encode('utf-8'))
                
                await writer.drain()
                
        except ConnectionResetError:
            pass
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug(f"Connection closed for {addr}")
    
    def _extract_frame(self, buffer: bytes):
        """Extract a complete RESP frame or inline command."""
        if not buffer:
            return None, buffer
        
        # RESP Array
        if buffer.startswith(b'*'):
            try:
                eol = buffer.find(b'\r\n')
                if eol == -1:
                    return None, buffer
                
                num_args = int(buffer[1:eol])
                pos = eol + 2
                
                for _ in range(num_args):
                    len_eol = buffer.find(b'\r\n', pos)
                    if len_eol == -1:
                        return None, buffer
                    
                    line = buffer[pos:len_eol]
                    if not line.startswith(b'$'):
                        return None, buffer
                    
                    arg_len = int(line[1:])
                    pos = len_eol + 2 + arg_len + 2
                    
                    if pos > len(buffer):
                        return None, buffer
                
                return buffer[:pos], buffer[pos:]
            except (ValueError, IndexError):
                pass
        
        # Inline command
        eol = buffer.find(b'\n')
        if eol != -1:
            return buffer[:eol+1], buffer[eol+1:]
        
        return None, buffer
    
    def _parse_command(self, raw: bytes):
        """Parse RESP or inline command into (cmd, args)."""
        if raw.startswith(b'*'):
            try:
                idx = 0
                
                def read_line(start):
                    end = raw.find(b'\r\n', start)
                    if end == -1:
                        return None, start
                    return raw[start:end], end + 2
                
                line, idx = read_line(idx)
                num_args = int(line[1:])
                
                parts = []
                for _ in range(num_args):
                    line, idx = read_line(idx)
                    if not line or not line.startswith(b'$'):
                        raise ValueError("Invalid RESP")
                    
                    arg_len = int(line[1:])
                    arg_data = raw[idx:idx + arg_len]
                    parts.append(arg_data)
                    idx += arg_len + 2
                
                if parts:
                    cmd = parts[0].decode('utf-8').upper()
                    args = [p.decode('latin-1') for p in parts[1:]]
                    return cmd, args
            except Exception:
                pass
        
        # Inline fallback
        parts = raw.strip().split()
        if not parts:
            raise ValueError("Empty command")
        
        cmd = parts[0].decode('utf-8').upper()
        args = [p.decode('utf-8') for p in parts[1:]]
        return cmd, args
    
    def _format_response(self, result: Any) -> str:
        """Format Python object as RESP response."""
        if result is None:
            return "$-1\r\n"
        elif isinstance(result, str):
            if result in ("OK", "QUEUED", "PONG"):
                return f"+{result}\r\n"
            return f"${len(result)}\r\n{result}\r\n"
        elif isinstance(result, bytes):
            decoded = result.decode('latin-1')
            return f"${len(decoded)}\r\n{decoded}\r\n"
        elif isinstance(result, int):
            return f":{result}\r\n"
        elif isinstance(result, float):
            return f"${len(str(result))}\r\n{result}\r\n"
        elif isinstance(result, list):
            parts = [f"*{len(result)}\r\n"]
            for item in result:
                parts.append(self._format_response(item))
            return "".join(parts)
        elif isinstance(result, Exception):
            return f"-ERR {str(result)}\r\n"
        else:
            s = str(result)
            return f"${len(s)}\r\n{s}\r\n"
    
    async def _execute_command(self, cmd: str, args: List[str]) -> Any:
        """Execute a command and return result."""
        
        # === String Commands ===
        if cmd == 'GET':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'get'")
            key = args[0].encode('utf-8')
            # Check TTL
            if key in self._ttls and time.time() > self._ttls[key]:
                await self.engine.delete(key)
                del self._ttls[key]
                return None
            return await self.engine.get(key)
        
        elif cmd == 'SET':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'set'")
            key = args[0].encode('utf-8')
            value = args[1].encode('utf-8')
            await self.engine.put(key, value)
            
            # Handle EX/PX options
            i = 2
            while i < len(args):
                opt = args[i].upper()
                if opt == 'EX' and i + 1 < len(args):
                    ttl = int(args[i + 1])
                    self._ttls[key] = time.time() + ttl
                    i += 2
                elif opt == 'PX' and i + 1 < len(args):
                    ttl_ms = int(args[i + 1])
                    self._ttls[key] = time.time() + ttl_ms / 1000
                    i += 2
                else:
                    i += 1
            return "OK"
        
        elif cmd == 'DEL' or cmd == 'DELETE':
            if len(args) < 1:
                raise ValueError("wrong number of arguments for 'del'")
            count = 0
            for arg in args:
                key = arg.encode('utf-8')
                if await self.engine.delete(key):
                    count += 1
                self._ttls.pop(key, None)
            return count
        
        elif cmd == 'EXISTS':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'exists'")
            key = args[0].encode('utf-8')
            val = await self.engine.get(key)
            return 1 if val is not None else 0
        
        elif cmd == 'EXPIRE':
            if len(args) != 2:
                raise ValueError("wrong number of arguments for 'expire'")
            key = args[0].encode('utf-8')
            seconds = int(args[1])
            val = await self.engine.get(key)
            if val is not None:
                self._ttls[key] = time.time() + seconds
                return 1
            return 0
        
        elif cmd == 'TTL':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'ttl'")
            key = args[0].encode('utf-8')
            if key not in self._ttls:
                val = await self.engine.get(key)
                return -1 if val is not None else -2
            remaining = self._ttls[key] - time.time()
            if remaining <= 0:
                await self.engine.delete(key)
                del self._ttls[key]
                return -2
            return int(remaining)
        
        elif cmd == 'PERSIST':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'persist'")
            key = args[0].encode('utf-8')
            if key in self._ttls:
                del self._ttls[key]
                return 1
            return 0
        
        # === List Commands ===
        elif cmd == 'LPUSH':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'lpush'")
            key = args[0].encode('utf-8')
            if key not in self._lists:
                self._lists[key] = []
            for val in args[1:]:
                self._lists[key].insert(0, val.encode('utf-8'))
            return len(self._lists[key])
        
        elif cmd == 'RPUSH':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'rpush'")
            key = args[0].encode('utf-8')
            if key not in self._lists:
                self._lists[key] = []
            for val in args[1:]:
                self._lists[key].append(val.encode('utf-8'))
            return len(self._lists[key])
        
        elif cmd == 'LRANGE':
            if len(args) != 3:
                raise ValueError("wrong number of arguments for 'lrange'")
            key = args[0].encode('utf-8')
            start = int(args[1])
            stop = int(args[2])
            lst = self._lists.get(key, [])
            if stop == -1:
                return lst[start:]
            return lst[start:stop + 1]
        
        elif cmd == 'LLEN':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'llen'")
            key = args[0].encode('utf-8')
            return len(self._lists.get(key, []))
        
        # === Hash Commands ===
        elif cmd == 'HSET':
            if len(args) != 3:
                raise ValueError("wrong number of arguments for 'hset'")
            key = args[0].encode('utf-8')
            field = args[1].encode('utf-8')
            value = args[2].encode('utf-8')
            if key not in self._hashes:
                self._hashes[key] = {}
            is_new = 1 if field not in self._hashes[key] else 0
            self._hashes[key][field] = value
            return is_new
        
        elif cmd == 'HGET':
            if len(args) != 2:
                raise ValueError("wrong number of arguments for 'hget'")
            key = args[0].encode('utf-8')
            field = args[1].encode('utf-8')
            h = self._hashes.get(key, {})
            return h.get(field)
        
        elif cmd == 'HGETALL':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'hgetall'")
            key = args[0].encode('utf-8')
            h = self._hashes.get(key, {})
            result = []
            for f, v in h.items():
                result.extend([f, v])
            return result
        
        elif cmd == 'HDEL':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'hdel'")
            key = args[0].encode('utf-8')
            h = self._hashes.get(key, {})
            count = 0
            for field in args[1:]:
                f = field.encode('utf-8')
                if f in h:
                    del h[f]
                    count += 1
            return count
        
        # === Set Commands ===
        elif cmd == 'SADD':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'sadd'")
            key = args[0].encode('utf-8')
            if key not in self._sets:
                self._sets[key] = set()
            count = 0
            for member in args[1:]:
                m = member.encode('utf-8')
                if m not in self._sets[key]:
                    self._sets[key].add(m)
                    count += 1
            return count
        
        elif cmd == 'SMEMBERS':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'smembers'")
            key = args[0].encode('utf-8')
            return list(self._sets.get(key, set()))
        
        elif cmd == 'SISMEMBER':
            if len(args) != 2:
                raise ValueError("wrong number of arguments for 'sismember'")
            key = args[0].encode('utf-8')
            member = args[1].encode('utf-8')
            return 1 if member in self._sets.get(key, set()) else 0
        
        elif cmd == 'SREM':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'srem'")
            key = args[0].encode('utf-8')
            s = self._sets.get(key, set())
            count = 0
            for member in args[1:]:
                m = member.encode('utf-8')
                if m in s:
                    s.remove(m)
                    count += 1
            return count
        
        # === Sorted Set Commands (v2) ===
        elif cmd == 'ZADD':
            if len(args) < 3 or len(args) % 2 == 0:
                raise ValueError("wrong number of arguments for 'zadd'")
            key = args[0].encode('utf-8')
            count = 0
            i = 1
            while i < len(args):
                score = float(args[i])
                member = args[i + 1].encode('utf-8')
                count += self.engine.zadd(key, score, member)
                i += 2
            return count
        
        elif cmd == 'ZRANGE':
            if len(args) < 3:
                raise ValueError("wrong number of arguments for 'zrange'")
            key = args[0].encode('utf-8')
            start = int(args[1])
            stop = int(args[2])
            return self.engine.zrange(key, start, stop)
        
        elif cmd == 'ZREVRANGE':
            if len(args) < 3:
                raise ValueError("wrong number of arguments for 'zrevrange'")
            key = args[0].encode('utf-8')
            start = int(args[1])
            stop = int(args[2])
            return self.engine.zrevrange(key, start, stop)
        
        elif cmd == 'ZRANGEBYSCORE':
            if len(args) < 3:
                raise ValueError("wrong number of arguments for 'zrangebyscore'")
            key = args[0].encode('utf-8')
            min_score = float('-inf') if args[1] == '-inf' else float(args[1])
            max_score = float('inf') if args[2] == '+inf' or args[2] == 'inf' else float(args[2])
            offset, limit = 0, -1
            i = 3
            while i < len(args):
                if args[i].upper() == 'LIMIT' and i + 2 < len(args):
                    offset = int(args[i + 1])
                    limit = int(args[i + 2])
                    i += 3
                else:
                    i += 1
            return self.engine.zrangebyscore(key, min_score, max_score, offset, limit)
        
        elif cmd == 'ZREVRANGEBYSCORE':
            if len(args) < 3:
                raise ValueError("wrong number of arguments for 'zrevrangebyscore'")
            key = args[0].encode('utf-8')
            max_score = float('inf') if args[1] == '+inf' or args[1] == 'inf' else float(args[1])
            min_score = float('-inf') if args[2] == '-inf' else float(args[2])
            offset, limit = 0, -1
            i = 3
            while i < len(args):
                if args[i].upper() == 'LIMIT' and i + 2 < len(args):
                    offset = int(args[i + 1])
                    limit = int(args[i + 2])
                    i += 3
                else:
                    i += 1
            return self.engine.zrevrangebyscore(key, max_score, min_score, offset, limit)
        
        elif cmd == 'ZSCORE':
            if len(args) != 2:
                raise ValueError("wrong number of arguments for 'zscore'")
            key = args[0].encode('utf-8')
            member = args[1].encode('utf-8')
            score = self.engine.zscore(key, member)
            return str(score) if score is not None else None
        
        elif cmd == 'ZREM':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'zrem'")
            key = args[0].encode('utf-8')
            count = 0
            for member in args[1:]:
                count += self.engine.zrem(key, member.encode('utf-8'))
            return count
        
        elif cmd == 'ZCARD':
            if len(args) != 1:
                raise ValueError("wrong number of arguments for 'zcard'")
            key = args[0].encode('utf-8')
            return self.engine.zcard(key)
        
        # === Full-Text Search Commands (v2) ===
        elif cmd == 'FT.ADD':
            if len(args) < 2:
                raise ValueError("wrong number of arguments for 'ft.add'")
            doc_id = args[0]
            text = ' '.join(args[1:])
            self.engine.ft_add(doc_id, text)
            return "OK"
        
        elif cmd == 'FT.SEARCH':
            if len(args) < 1:
                raise ValueError("wrong number of arguments for 'ft.search'")
            query = args[0]
            limit = 20
            i = 1
            while i < len(args):
                if args[i].upper() == 'LIMIT' and i + 1 < len(args):
                    limit = int(args[i + 1])
                    i += 2
                else:
                    i += 1
            results = self.engine.ft_search(query, limit)
            # Format as [doc_id, score, doc_id, score, ...]
            flat = []
            for doc_id, score in results:
                flat.extend([doc_id.encode('utf-8'), str(score).encode('utf-8')])
            return flat
        
        # === Cache Stats (v2) ===
        elif cmd == 'CACHE.STATS':
            stats = self.engine.get_stats()
            lines = []
            for k, v in stats.items():
                if isinstance(v, dict):
                    for sk, sv in v.items():
                        lines.append(f"{k}.{sk}: {sv}")
                else:
                    lines.append(f"{k}: {v}")
            return '\n'.join(lines)
        
        # === Utility Commands ===
        elif cmd == 'PING':
            return "PONG" if not args else args[0]
        
        elif cmd == 'INFO':
            stats = self.engine.get_stats()
            lines = [
                "# Server",
                f"ignisdb_version:2.0.0",
                f"uptime_in_seconds:{int(time.time())}",
                "",
                "# Storage",
                f"memtable_entries:{stats['memtable_entries']}",
                f"sstable_count:{stats['sstable_count']}",
                f"total_writes:{stats['writes']}",
                f"total_reads:{stats['reads']}",
                "",
                "# Cache",
                f"cache_hits:{stats['cache']['hits']}",
                f"cache_misses:{stats['cache']['misses']}",
                f"cache_hit_rate:{stats['cache']['hit_rate_pct']}%",
                f"cache_memory:{stats['cache']['hot_cache_bytes']}",
            ]
            return '\n'.join(lines)
        
        elif cmd == 'AUTH':
            # Placeholder - auth handled at connection level
            return "OK"
        
        elif cmd == 'MULTI':
            return "OK"  # Transaction placeholder
        
        elif cmd == 'EXEC':
            return []  # Transaction placeholder
        
        elif cmd == 'DISCARD':
            return "OK"
        
        elif cmd == 'SUBSCRIBE':
            return "OK"  # PubSub placeholder
        
        elif cmd == 'PUBLISH':
            return 0  # PubSub placeholder
        
        else:
            raise ValueError(f"unknown command '{cmd}'")
