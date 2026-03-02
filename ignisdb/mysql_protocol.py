import struct
import logging
import asyncio
from typing import Tuple, Any, List

logger = logging.getLogger(__name__)

# MySQL Constants
MYSQL_TYPE_VAR_STRING = 0xFD
MYSQL_TYPE_LONG = 0x03
MYSQL_TYPE_LONGLONG = 0x08
COM_QUERY = 0x03
COM_QUIT = 0x01
COM_INIT_DB = 0x02
SERVER_VERSION = "5.7.0-IgnisDB" # Pretend to be MySQL 5.7
CONNECTION_ID = 1

class MySQLProtocolHandler:
    """
    Handles MySQL Wire Protocol interactions.
    """
    def __init__(self, reader, writer, storage):
        self.reader = reader
        self.writer = writer
        self.storage = storage
        self.sequence_id = 0
        self.authenticated = False
        self.current_db = None

    async def handle_connection(self):
        """Main loop for handling a MySQL connection."""
        try:
            # 1. Send Handshake
            await self.send_handshake()
            
            # 2. Read Client Response (Auth)
            packet = await self.read_packet()
            if not packet: return
            
            # For MVP, we skip real auth verification and just accept 'root' or whatever
            # We assume the user is 'lirakazan_admin' as requested
            self.send_ok_packet()
            self.authenticated = True
            
            # 3. Command Phase
            while True:
                packet = await self.read_packet()
                if not packet: break
                
                command = packet[0]
                payload = packet[1:]
                
                if command == COM_QUIT:
                    break
                
                elif command == COM_INIT_DB:
                    self.current_db = payload.decode('utf-8', errors='ignore')
                    self.send_ok_packet()
                    
                elif command == 0x0E: # COM_PING
                    self.send_ok_packet()

                elif command == COM_QUERY:
                    query = payload.decode('utf-8', errors='ignore')
                    await self.handle_query(query)
                
                else:
                    logger.warning(f"Unsupported MySQL Command: {command}")
                    self.send_err_packet(1047, "Unknown command")

        except Exception as e:
            logger.error(f"MySQL Protocol Error: {e}")
        finally:
            self.writer.close()

    async def handle_query(self, query):
        """Parses SQL and maps to IgnisDB."""
        # logger.info(f"MySQL Query: {query}") # Too verbose for high throughput
        
        query_upper = query.strip().upper()
        
        if "SET NAMES" in query_upper:
             self.send_ok_packet()
             return

        try:
            import re
            import json

            # --- SELECT (GET) ---
            if query_upper.startswith("SELECT"):
                # SELECT * FROM `table` WHERE `id` = 'val'
                match = re.search(r"FROM\s+[`]?(\w+)[`]?\s+WHERE\s+[`]?(\w+)[`]?\s*=\s*['\"]?(\w+)['\"]?", query, re.IGNORECASE)
                
                if match:
                    table, col, val = match.groups()
                    if col.lower() != 'id':
                         # Only support lookup by ID for now
                         self.send_err_packet(1054, f"Unknown column '{col}' in 'where clause' (Only 'id' supported)")
                         return

                    key = f"{table}:{val}"
                    data = await self.storage.get(key)
                    
                    if data:
                        try:
                            row_data = json.loads(data) if isinstance(data, str) else {"value": data}
                        except:
                            row_data = {"value": data}
                        
                        columns = list(row_data.keys())
                        rows = [list(row_data.values())]
                        await self.send_result_set(columns, rows)
                    else:
                        await self.send_result_set(["id"], [])
                else:
                    await self.send_result_set(["info"], [["IgnisDB: Complex SELECT not supported"]])

            # --- INSERT (SET) ---
            elif query_upper.startswith("INSERT"):
                # INSERT INTO `table` (`id`, `col1`) VALUES ('1', 'val1')
                # Simplification: Expecting explicit ID in columns
                table_match = re.search(r"INTO\s+[`]?(\w+)[`]?\s*", query, re.IGNORECASE)
                if table_match:
                    table = table_match.group(1)
                    
                    # Extract Columns
                    cols_match = re.search(r"\(([\w`,\s]+)\)\s*VALUES", query, re.IGNORECASE)
                    # Extract Values
                    vals_match = re.search(r"VALUES\s*\((.+)\)", query, re.IGNORECASE)
                    
                    if cols_match and vals_match:
                        raw_cols = [c.strip().strip('`') for c in cols_match.group(1).split(',')]
                        # Split values by comma, respecting quotes is hard with simple split. 
                        # Hacky split for MVP:
                        raw_vals = [v.strip().strip("'").strip('"') for v in vals_match.group(1).split(',')]
                        
                        if len(raw_cols) != len(raw_vals):
                            self.send_err_packet(1136, "Column count doesn't match value count")
                            return
                        
                        row_data = dict(zip(raw_cols, raw_vals))
                        
                        if 'id' not in row_data:
                             self.send_err_packet(1364, "Field 'id' doesn't have a default value")
                             return
                        
                        id_val = row_data['id']
                        key = f"{table}:{id_val}"
                        
                        # Store as JSON
                        await self.storage.set(key, json.dumps(row_data))
                        self.send_ok_packet()
                    else:
                        self.send_err_packet(1064, "Parse error in INSERT")
                else:
                     self.send_err_packet(1064, "Parse error in INSERT (table)")

            # --- UPDATE (GET -> MERGE -> SET) ---
            elif query_upper.startswith("UPDATE"):
                # UPDATE `table` SET `col`='val' WHERE `id`='1'
                table_match = re.search(r"UPDATE\s+[`]?(\w+)[`]?\s+SET", query, re.IGNORECASE)
                where_match = re.search(r"WHERE\s+[`]?id[`]?\s*=\s*['\"]?(\w+)['\"]?", query, re.IGNORECASE)
                
                if table_match and where_match:
                    table = table_match.group(1)
                    id_val = where_match.group(1)
                    key = f"{table}:{id_val}"
                    
                    # 1. Get existing
                    existing_data = await self.storage.get(key)
                    if not existing_data:
                        # MySQL returns OK with 0 affected rows if not found
                        self.send_ok_packet() 
                        return

                    try:
                        current_row = json.loads(existing_data) if isinstance(existing_data, str) else {"value": existing_data}
                    except:
                        current_row = {"value": existing_data}

                    # 2. Parse SET clause
                    # SET `col1`='val1', `col2`='val2'
                    set_clause_match = re.search(r"SET\s+(.+)\s+WHERE", query, re.IGNORECASE)
                    if set_clause_match:
                        assignments = set_clause_match.group(1).split(',')
                        for assign in assignments:
                            parts = assign.split('=')
                            if len(parts) == 2:
                                col = parts[0].strip().strip('`')
                                val = parts[1].strip().strip("'").strip('"')
                                current_row[col] = val
                        
                        # 3. Save back
                        await self.storage.set(key, json.dumps(current_row))
                        self.send_ok_packet()
                    else:
                        self.send_err_packet(1064, "Parse error in UPDATE (SET clause)")
                else:
                    self.send_err_packet(1064, "Parse error in UPDATE")

            # --- DELETE (DELETE) ---
            elif query_upper.startswith("DELETE"):
                # DELETE FROM `table` WHERE `id`='1'
                match = re.search(r"FROM\s+[`]?(\w+)[`]?\s+WHERE\s+[`]?id[`]?\s*=\s*['\"]?(\w+)['\"]?", query, re.IGNORECASE)
                if match:
                    table, id_val = match.groups()
                    key = f"{table}:{id_val}"
                    
                    deleted = await self.storage.delete(key)
                    # Send OK with affected rows? Packet doesn't strictly require it for simple OK
                    self.send_ok_packet() 
                else:
                    self.send_err_packet(1064, "Parse error in DELETE")

            else:
                 # Fallback
                 self.send_err_packet(1064, "Unrecognized SQL Command")
                    
        except Exception as e:
            logger.error(f"Query Error: {e}")
            self.send_err_packet(500, str(e))

    # --- Packet Construction Helpers ---
    
    async def send_handshake(self):
        # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
        payload = b''
        payload += b'\x0a' # Protocol version 10
        payload += SERVER_VERSION.encode('utf-8') + b'\x00'
        payload += struct.pack('<I', CONNECTION_ID)
        payload += b'12345678' + b'\x00' # Auth Plugin Data Part 1 (salt)
        payload += b'\xff\xf7' # Capabilities (Lower)
        payload += b'\x08' # Charset (utf8mb4)
        payload += b'\x02\x00' # Status Flags
        payload += b'\x0f\x80' # Capabilities (Upper)
        payload += b'\x15' # Auth Plugin Data Length
        payload += b'\x00' * 10 # Reserved
        payload += b'123456789012' + b'\x00' # Auth Plugin Data Part 2
        payload += b'mysql_native_password' + b'\x00' # Auth Plugin Name
        
        await self.write_packet(payload)

    def send_ok_packet(self):
        payload = b'\x00' # Header: OK
        payload += b'\x00' # Affected rows
        payload += b'\x00' # Last insert ID
        payload += b'\x02\x00' # Status flags
        payload += b'\x00\x00' # Warnings
        asyncio.create_task(self.write_packet(payload))

    def send_err_packet(self, error_code, error_msg):
        payload = b'\xff' # Header: ERR
        payload += struct.pack('<H', error_code)
        payload += b'#'
        payload += b'HY000' # SQL State
        payload += error_msg.encode('utf-8')
        asyncio.create_task(self.write_packet(payload))

    async def send_result_set(self, columns, rows):
        # 1. Column Count Packet
        await self.write_packet(struct.pack('<B', len(columns)))
        
        # 2. Column Definition Packets
        for col_name in columns:
            await self.write_packet(self.pack_column_def(col_name))
            
        # 3. EOF Packet
        await self.write_packet(b'\xfe\x00\x00\x02\x00')
        
        # 4. Row Data Packets
        for row in rows:
            await self.write_packet(self.pack_row(row))
            
        # 5. EOF Packet
        await self.write_packet(b'\xfe\x00\x00\x02\x00')

    def pack_column_def(self, name):
        # Simplification
        payload = b''
        payload += self.pack_lenenc_str("def") # Catalog
        payload += self.pack_lenenc_str("ignis") # Schema
        payload += self.pack_lenenc_str("tbl") # Table
        payload += self.pack_lenenc_str("tbl") # Org Table
        payload += self.pack_lenenc_str(name) # Name
        payload += self.pack_lenenc_str(name) # Org Name
        payload += b'\x0c' # Length of fixed fields
        payload += b'\x08\x00' # Charset
        payload += b'\xff\x00\x00\x00' # Column length
        payload += b'\xfd' # Type (VarString)
        payload += b'\x00\x00' # Flags
        payload += b'\x00' # Decimals
        payload += b'\x00\x00' # Filler
        return payload

    def pack_row(self, row):
        payload = b''
        for val in row:
             if val is None:
                 payload += b'\xfb'
             else:
                 payload += self.pack_lenenc_str(str(val))
        return payload

    def pack_lenenc_str(self, s):
        data = s.encode('utf-8')
        length = len(data)
        if length < 251:
            return struct.pack('<B', length) + data
        elif length < 65536:
            return b'\xfc' + struct.pack('<H', length) + data
        elif length < 16777216:
            return b'\xfd' + struct.pack('<I', length)[:3] + data
        else:
            return b'\xfe' + struct.pack('<Q', length) + data

    async def write_packet(self, payload):
        length = len(payload)
        header = struct.pack('<I', length)
        # Sequence ID logic is tricky in async, simpler for now
        # MySQL packet header is 3 bytes length + 1 byte sequence
        header_bytes = header[:3] + struct.pack('<B', self.sequence_id)
        self.writer.write(header_bytes + payload)
        await self.writer.drain()
        self.sequence_id = (self.sequence_id + 1) % 256

    async def read_packet(self):
        header = await self.reader.read(4)
        if len(header) < 4: return None
        
        length_bytes = header[:3]
        sequence_id = header[3]
        length = struct.unpack('<I', length_bytes + b'\x00')[0]
        
        self.sequence_id = (sequence_id + 1) % 256
        
        payload = await self.reader.read(length)
        if len(payload) < length: return None
        return payload
