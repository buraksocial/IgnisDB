"""Tests for RESP parsing and reply formatting."""
from ignisdb.exceptions import CommandError, WrongTypeError
from ignisdb.protocol import ProtocolHandler


def handler():
    return ProtocolHandler()


# --- Replies -------------------------------------------------------------

def test_nil_is_a_resp_null_bulk_string():
    """`_(nil)` is not a RESP type; clients desynchronise on it."""
    assert handler().format_response(None) == b"$-1\r\n"


def test_simple_status_reply():
    assert handler().format_response("OK") == b"+OK\r\n"
    assert handler().format_response("QUEUED") == b"+QUEUED\r\n"


def test_integer_reply():
    assert handler().format_response(2) == b":2\r\n"
    assert handler().format_response(0) == b":0\r\n"


def test_bulk_string_length_counts_bytes_not_characters():
    """`José` is 4 characters but 5 UTF-8 bytes.

    Declaring the character count leaves a trailing byte in the stream and
    every following reply is read at the wrong offset.
    """
    reply = handler().format_response("José")
    assert reply == b"$5\r\nJos\xc3\xa9\r\n"

    header, _, body = reply.partition(b"\r\n")
    assert int(header[1:]) == len(body) - 2


def test_bulk_string_length_for_emoji():
    reply = handler().format_response("🔥")
    assert reply == b"$4\r\n\xf0\x9f\x94\xa5\r\n"


def test_array_reply_is_nested():
    assert handler().format_response(["a", "b"]) == b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"


def test_empty_array_reply():
    assert handler().format_response([]) == b"*0\r\n"


def test_array_containing_nil():
    assert handler().format_response(["a", None]) == b"*2\r\n$1\r\na\r\n$-1\r\n"


def test_bytes_reply_is_passed_through():
    assert handler().format_response(b"\x00\xff") == b"$2\r\n\x00\xff\r\n"


# --- Errors --------------------------------------------------------------

def test_error_code_is_not_doubled():
    """Most commands raise ValueError('ERR ...'), which used to become
    '-ERR ERR wrong number of arguments'."""
    error = ValueError("ERR wrong number of arguments for 'get' command")
    assert handler().format_response(error) == (
        b"-ERR wrong number of arguments for 'get' command\r\n"
    )


def test_error_without_code_gets_one():
    error = CommandError("Unknown command 'nope'")
    assert handler().format_response(error) == b"-ERR Unknown command 'nope'\r\n"


def test_wrongtype_error_uses_its_own_code():
    error = WrongTypeError("Operation against a key holding the wrong kind of value")
    assert handler().format_response(error) == (
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    )


def test_existing_error_code_is_preserved():
    error = CommandError("NOAUTH Authentication required.")
    assert handler().format_response(error) == b"-NOAUTH Authentication required.\r\n"


def test_error_message_stays_on_one_line():
    error = CommandError("broken\r\nINJECTED")
    reply = handler().format_response(error)
    assert reply.count(b"\r\n") == 1
    assert reply.endswith(b"\r\n")


# --- Parsing -------------------------------------------------------------

def test_parse_inline_command():
    cmd, args = handler().parse_command(b"SET foo bar\r\n")
    assert (cmd, args) == ("SET", ["foo", "bar"])


def test_parse_resp_command():
    raw = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    cmd, args = handler().parse_command(raw)
    assert (cmd, args) == ("SET", ["foo", "bar"])


def test_parse_decodes_utf8_arguments():
    """Args were decoded as latin-1 while replies were encoded as UTF-8,
    so `José` came back as `JosÃ©`."""
    raw = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nJos\xc3\xa9\r\n"
    _, args = handler().parse_command(raw)
    assert args == ["k", "José"]


def test_value_survives_a_parse_format_roundtrip():
    protocol = handler()
    raw = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nJos\xc3\xa9\r\n"
    _, args = protocol.parse_command(raw)
    assert protocol.format_response(args[1]) == b"$5\r\nJos\xc3\xa9\r\n"


def test_binary_payload_survives_a_roundtrip():
    protocol = handler()
    payload = b"\x00\x01\xfe\xff"
    raw = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n" + payload + b"\r\n"
    _, args = protocol.parse_command(raw)
    assert protocol.format_response(args[1]) == b"$4\r\n" + payload + b"\r\n"


def test_empty_command_is_rejected():
    try:
        handler().parse_command(b"   \r\n")
    except CommandError:
        return
    raise AssertionError("expected CommandError for an empty command")


# --- Framing -------------------------------------------------------------

def test_extract_frame_returns_none_for_partial_resp():
    buffer = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nba"
    frame, remainder = handler().extract_frame(buffer)
    assert frame is None
    assert remainder == buffer


def test_extract_frame_splits_pipelined_commands():
    buffer = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
    frame, remainder = handler().extract_frame(buffer)
    assert frame == b"*1\r\n$4\r\nPING\r\n"
    assert remainder == b"*1\r\n$4\r\nPING\r\n"


def test_extract_frame_handles_inline():
    frame, remainder = handler().extract_frame(b"GET foo\nGET bar\n")
    assert frame == b"GET foo\n"
    assert remainder == b"GET bar\n"


# --- Replication payloads -----------------------------------------------

def test_format_command_as_bytes_counts_bytes():
    out = handler().format_command_as_bytes("SET", "k", "José")
    assert out == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nJos\xc3\xa9\r\n"


def test_format_command_as_bytes_survives_values_with_spaces():
    """Inline framing loses the value here; RESP carries it intact."""
    protocol = handler()
    out = protocol.format_command_as_bytes("SET", "k", "hello world")
    cmd, args = protocol.parse_command(out)
    assert (cmd, args) == ("SET", ["k", "hello world"])
