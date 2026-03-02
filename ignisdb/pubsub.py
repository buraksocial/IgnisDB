import asyncio
import logging
from typing import Dict, Set, Any

logger = logging.getLogger(__name__)

class PubSubManager:
    """Manages Publish/Subscribe channels and subscribers."""
    def __init__(self):
        # Channel -> Set of writers (client connections)
        self.channels: Dict[str, Set[Any]] = {}

    def subscribe(self, channel: str, writer: Any):
        """Subscribes a client to a channel."""
        if channel not in self.channels:
            self.channels[channel] = set()
        self.channels[channel].add(writer)
        logger.debug(f"Client subscribed to {channel}")

    def unsubscribe(self, channel: str, writer: Any):
        """Unsubscribes a client from a channel."""
        if channel in self.channels:
            self.channels[channel].discard(writer)
            if not self.channels[channel]:
                del self.channels[channel]
            logger.debug(f"Client unsubscribed from {channel}")

    async def publish(self, channel: str, message: str) -> int:
        """Publishes a message to a channel. Returns number of clients receiving it."""
        if channel not in self.channels:
            return 0
        
        count = 0
        # Format: *3\r\n$7\r\nmessage\r\n$channel_len\r\nchannel\r\n$msg_len\r\nmessage\r\n
        # RESP array of 3: ["message", channel, message payload]
        # This allows clients to know which subscription triggered this.
        resp_msg = f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n"
        encoded_msg = resp_msg.encode('utf-8')
        
        subscribers = list(self.channels[channel])
        for writer in subscribers:
            try:
                if writer.is_closing():
                    self.unsubscribe(channel, writer)
                    continue
                writer.write(encoded_msg)
                await writer.drain()
                count += 1
            except Exception as e:
                logger.error(f"Error publishing to client: {e}")
                self.unsubscribe(channel, writer)
                
        return count

    def remove_client(self, writer: Any):
        """Removes a client from all subscriptions (on disconnect)."""
        for channel in list(self.channels.keys()):
            if writer in self.channels[channel]:
                self.channels[channel].discard(writer)
                if not self.channels[channel]:
                    del self.channels[channel]
