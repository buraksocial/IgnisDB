import json
import logging
from typing import Tuple, List, Any, Optional
from .exceptions import CommandError, WrongTypeError

logger = logging.getLogger(__name__)

class ProtocolHandler:
    """Parses raw client data into commands and formats responses into RESP."""
    
    def extract_frame(self, buffer: bytes) -> Tuple[Optional[bytes], bytes]:
        """
        Extracts a complete command frame from the buffer.
        Returns (frame, remainder). If incomplete, returns (None, buffer).
        Supports both RESP arrays and Inline commands.
        """
        if not buffer:
            return None, buffer
            
        # Check for RESP Array
        if buffer.startswith(b'*'):
            try:
                eol = buffer.find(b'\r\n')
                if eol == -1: return None, buffer
                
                num_args = int(buffer[1:eol])
                current_pos = eol + 2
                
                for _ in range(num_args):
                    len_eol = buffer.find(b'\r\n', current_pos)
                    if len_eol == -1: return None, buffer
                    
                    line = buffer[current_pos : len_eol]
                    if not line.startswith(b'$'): return None, buffer
                    
                    arg_len = int(line[1:])
                    current_pos = len_eol + 2
                    current_pos += arg_len + 2  # Skip data + \r\n
                    
                    if current_pos > len(buffer): return None, buffer
                    
                # Full RESP frame found
                return buffer[:current_pos], buffer[current_pos:]
            except (ValueError, IndexError):
                pass  # Fall through to inline
        
        # Fallback: Inline (newline-delimited)
        eol = buffer.find(b'\n')
        if eol != -1:
            return buffer[:eol+1], buffer[eol+1:]
            
        return None, buffer

    def parse_command(self, command_raw: bytes) -> Tuple[str, List[str]]:
        """Parses a raw bytes command into a (command, [args]) tuple. Decodes args to utf-8 strings."""
        
        # RESP Array parsing (length-prefixed, binary-safe)
        if command_raw.startswith(b'*'):
            try:
                idx = 0
                
                def read_line(start):
                    end = command_raw.find(b'\r\n', start)
                    if end == -1: return None, start
                    return command_raw[start:end], end + 2
                
                line, idx = read_line(idx)
                if line is None: raise ValueError("Incomplete RESP")
                num_args = int(line[1:])
                
                parts = []
                for _ in range(num_args):
                    line, idx = read_line(idx)
                    if line is None or not line.startswith(b'$'):
                        raise ValueError("Invalid RESP arg header")
                    
                    arg_len = int(line[1:])
                    if idx + arg_len > len(command_raw):
                        raise ValueError("Incomplete RESP body")
                    
                    arg_data = command_raw[idx : idx + arg_len]
                    parts.append(arg_data)
                    idx += arg_len + 2  # Skip data + \r\n
                    
                if parts:
                    # Decode all args to string using latin-1 (lossless for 0-255 byte values)
                    cmd = parts[0].decode('utf-8')
                    args = [p.decode('latin-1') for p in parts[1:]]
                    return cmd, args
                    
            except Exception:
                pass  # Fallback to inline
        
        # Inline: space-separated
        parts = command_raw.strip().split()
        if not parts:
            raise CommandError("Empty command")
        
        return parts[0].decode('utf-8'), [p.decode('utf-8') for p in parts[1:]]

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
            response_parts = [f"*{len(result)}\r\n"]
            for item in result:
                response_parts.append(self.format_response(item))
            return "".join(response_parts)
        elif isinstance(result, dict):
            # Serialize dictionary as JSON string
            json_str = json.dumps(result, ensure_ascii=False)
            return f"${len(json_str)}\r\n{json_str}\r\n"
        elif isinstance(result, Exception):
            err_type = "WRONGTYPE" if isinstance(result, WrongTypeError) else "ERR"
            return f"-{err_type} {str(result)}\r\n"
        else:
            logger.error(f"Cannot format unknown response type: {type(result)}")
            return f"-ERR Server error: cannot format response\r\n"

    def format_command_as_bytes(self, command: str, *args: Any) -> bytes:
        """Formats a command and arguments into a RESP byte string (for replication)."""
        parts = [f"*{len(args) + 1}\r\n", f"${len(command)}\r\n{command}\r\n"]
        for arg in args:
            arg_str = str(arg)
            parts.append(f"${len(arg_str)}\r\n{arg_str}\r\n")
        return "".join(parts).encode('utf-8')
