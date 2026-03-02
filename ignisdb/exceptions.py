class IgnisError(Exception):
    """Base exception for IgnisDB."""
    pass

class CommandError(IgnisError):
    """Exception for command-related errors (e.g. unknown command, wrong args)."""
    pass

class WrongTypeError(IgnisError, TypeError):
    """Exception for operation against a key holding the wrong kind of value."""
    pass
