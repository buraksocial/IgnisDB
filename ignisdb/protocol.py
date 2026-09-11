import json
import logging
from typing import Tuple, List, Any, Optional
from .exceptions import CommandError, WrongTypeError

logger = logging.getLogger(__name__)

# Error replies start with an uppercase code. If an exception message already
# carries one we must not prefix another, or clients see "-ERR ERR ...".
ERROR_CODES = frozenset({
    "ERR", "WRONGTYPE", "NOAUTH", "WRONGPASS", "NOPERM", "NOSCRIPT",
    "BUSYGROUP", "EXECABORT", "READONLY", "MOVED", "ASK", "LOADING",
})


class ProtocolHandler:
    """Parses raw client data into commands and formats responses into RESP.

    Strings cross the wire as UTF-8. Byte values that are not valid UTF-8 are
    carried through Python's `surrogateescape` handler, so arbitrary binary
    payloads survive a SET/GET round trip untouched.
    """

    ENCODING = 'utf-8'
    ENCODING_ERRORS = 'surrogateescape'

    def _encode(self, value: str) -> bytes:
        return value.encode(self.ENCODING, self.ENCODING_ERRORS)

    def _decode(self, value: bytes) -> str:
        return value.decode(self.ENCODING, self.ENCODING_ERRORS)

    def _bulk(self, payload: bytes) -> bytes:
        """Builds a RESP bulk string with a length counted in BYTES, not characters."""
        return b"$%d\r\n%s\r\n" % (len(payload), payload)

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
                    # One decoding scheme for every path: UTF-8 with
                    # surrogateescape. Decoding args as latin-1 here while
                    # responses were encoded as UTF-8 turned every non-ASCII
                    # value into mojibake on the way back out.
                    cmd = self._decode(parts[0])
                    args = [self._decode(p) for p in parts[1:]]
                    return cmd, args

            except Exception:
                pass  # Fallback to inline

        # Inline: space-separated
        parts = command_raw.strip().split()
        if not parts:
            raise CommandError("Empty command")

        return self._decode(parts[0]), [self._decode(p) for p in parts[1:]]

    def format_response(self, result: Any) -> bytes:
        """Formats a Python object into a RESP reply for the client.

        Returns bytes rather than str: bulk-string lengths must count bytes, so
        the reply cannot be built as text and encoded afterwards without the
        declared length disagreeing with the payload for any non-ASCII value.
        """
        if result is None:
            # RESP null bulk string. The previous "_(nil)" is not a RESP type
            # at all and desynchronises every standard client.
            return b"$-1\r\n"
        elif isinstance(result, bool):
            # Must precede the int branch: bool is a subclass of int.
            return b":1\r\n" if result else b":0\r\n"
        elif isinstance(result, str):
            if result == "OK" or result == "QUEUED":
                return b"+%s\r\n" % self._encode(result)
            return self._bulk(self._encode(result))
        elif isinstance(result, (bytes, bytearray)):
            return self._bulk(bytes(result))
        elif isinstance(result, int):
            return b":%d\r\n" % result
        elif isinstance(result, list):
            response_parts = [b"*%d\r\n" % len(result)]
            for item in result:
                response_parts.append(self.format_response(item))
            return b"".join(response_parts)
        elif isinstance(result, dict):
            # Serialize dictionary as JSON string
            json_str = json.dumps(result, ensure_ascii=False)
            return self._bulk(self._encode(json_str))
        elif isinstance(result, Exception):
            return self.format_error(result)
        else:
            logger.error(f"Cannot format unknown response type: {type(result)}")
            return b"-ERR Server error: cannot format response\r\n"

    def format_error(self, exc: Exception) -> bytes:
        """Formats an exception as a RESP error, without doubling the error code."""
        message = str(exc).strip() or "unknown error"
        first_word = message.split(" ", 1)[0]

        if isinstance(exc, WrongTypeError) and first_word != "WRONGTYPE":
            message = f"WRONGTYPE {message}"
        elif first_word not in ERROR_CODES:
            message = f"ERR {message}"

        # An error reply is a single line: newlines would break framing.
        message = message.replace("\r", " ").replace("\n", " ")
        return b"-%s\r\n" % self._encode(message)

    def format_command_as_bytes(self, command: str, *args: Any) -> bytes:
        """Formats a command and arguments into a RESP byte string (for replication)."""
        parts = [b"*%d\r\n" % (len(args) + 1), self._bulk(self._encode(command))]
        for arg in args:
            if isinstance(arg, (bytes, bytearray)):
                parts.append(self._bulk(bytes(arg)))
            else:
                parts.append(self._bulk(self._encode(str(arg))))
        return b"".join(parts)
