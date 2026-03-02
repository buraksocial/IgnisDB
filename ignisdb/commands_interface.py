from abc import ABC, abstractmethod
from typing import List, Any
import logging

from dataclasses import dataclass

@dataclass
class ServerContext:
    storage: Any
    pubsub: Any = None
    writer: Any = None
    aof: Any = None
    server: Any = None
    security: Any = None

class Command(ABC):
    """Abstract base class for all IgnisDB commands."""
    
    @abstractmethod
    async def execute(self, context: ServerContext, *args: Any):
        """Executes the command against the server context."""
        pass

class CommandRegistry:
    """Registry for all available commands."""
    _commands = {}

    @classmethod
    def register(cls, name: str):
        def decorator(command_cls):
            cls._commands[name.upper()] = command_cls
            return command_cls
        return decorator

    @classmethod
    def get_command(cls, name: str):
        return cls._commands.get(name.upper())
