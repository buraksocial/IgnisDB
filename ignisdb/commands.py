import base64
from .commands_interface import Command, CommandRegistry
from .exceptions import CommandError

@CommandRegistry.register('AUTH')
class AuthCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 1:
            raise CommandError("ERR wrong number of arguments for 'auth' command")
        return "OK"

@CommandRegistry.register('BGREWRITEAOF')
class BgRewriteAofCommand(Command):
    async def execute(self, context, *args):
        # Logic handled in server.py mainly, but let's formalize if we can.
        # Ideally server invokes this. For now keeping "OK" as placeholder or 
        # allowing server interception.
        return "OK"

@CommandRegistry.register('GET')
class GetCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 1: raise ValueError("ERR wrong number of arguments for 'get' command")
        return await context.storage.get(args[0])

@CommandRegistry.register('SET')
class SetCommand(Command):
    async def execute(self, context, *args):
        if len(args) < 2: raise ValueError("ERR wrong number of arguments for 'set' command")
        key, value = args[0], args[1]
        expire = None
        if len(args) > 2:
            try:
                expire = int(args[2])
            except ValueError:
                pass 
        
        return await context.storage.set(key, value, expire)

@CommandRegistry.register('DELETE')
class DeleteCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 1: raise ValueError("ERR wrong number of arguments for 'delete' command")
        return await context.storage.delete(args[0])

@CommandRegistry.register('EXPIRE')
class ExpireCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 2: raise ValueError("ERR wrong number of arguments for 'expire' command")
        return await context.storage.expire(args[0], int(args[1]))

@CommandRegistry.register('LPUSH')
class LPushCommand(Command):
    async def execute(self, context, *args):
        if len(args) < 2: raise ValueError("ERR wrong number of arguments for 'lpush' command")
        key = args[0]
        values = list(args[1:])
        return await context.storage.lpush(key, values)

@CommandRegistry.register('LRANGE')
class LRangeCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 3: raise ValueError("ERR wrong number of arguments for 'lrange' command")
        return await context.storage.lrange(args[0], int(args[1]), int(args[2]))

@CommandRegistry.register('HSET')
class HSetCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 3: raise ValueError("ERR wrong number of arguments for 'hset' command")
        return await context.storage.hset(args[0], args[1], args[2])

@CommandRegistry.register('HGET')
class HGetCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 2: raise ValueError("ERR wrong number of arguments for 'hget' command")
        return await context.storage.hget(args[0], args[1])

@CommandRegistry.register('SADD')
class SAddCommand(Command):
    async def execute(self, context, *args):
        if len(args) < 2: raise ValueError("ERR wrong number of arguments for 'sadd' command")
        return await context.storage.sadd(args[0], list(args[1:]))

@CommandRegistry.register('SREM')
class SRemCommand(Command):
    async def execute(self, context, *args):
        if len(args) < 2: raise ValueError("ERR wrong number of arguments for 'srem' command")
        return await context.storage.srem(args[0], list(args[1:]))

@CommandRegistry.register('SMEMBERS')
class SMembersCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 1: raise ValueError("ERR wrong number of arguments for 'smembers' command")
        return await context.storage.smembers(args[0])

@CommandRegistry.register('PUBLISH')
class PublishCommand(Command):
    async def execute(self, context, *args):
        if len(args) != 2: raise ValueError("ERR wrong number of arguments for 'publish' command")
        channel, message = args[0], args[1]
        if not context.pubsub:
            return 0
        return await context.pubsub.publish(channel, message)

@CommandRegistry.register('SYNC')
class SyncCommand(Command):
    async def execute(self, context, *args):
        return "OK"

@CommandRegistry.register('REPLICAOF')
class ReplicaOfCommand(Command):
    async def execute(self, context, *args):
        return "OK"

@CommandRegistry.register('EXPORT')
class ExportCommand(Command):
    async def execute(self, context, *args):
        # Logic handled in server.py (needs access to StorageEngine and FS)
        return "OK"

@CommandRegistry.register('IMPORT')
class ImportCommand(Command):
    async def execute(self, context, *args):
        # Logic handled in server.py
        return "OK"

@CommandRegistry.register('SETBLOB')
class SetBlobCommand(Command):
    async def execute(self, context, *args):
        """
        SETBLOB key <base64_raw_data> [COMPRESS] [ENCRYPT] [EX seconds]
        
        Stores binary data with optional compression and encryption.
        The client sends raw Base64 of the original bytes.
        Server applies: compress → encrypt → re-encode to Base64.
        """
        if len(args) < 2: raise ValueError("ERR wrong number of arguments for 'setblob' command")
        key, raw_b64 = args[0], args[1]
        
        # Parse flags
        do_compress = True   # Default: always compress
        do_encrypt = False
        expire = None
        
        i = 2
        while i < len(args):
            flag = args[i].upper()
            if flag == 'COMPRESS':
                do_compress = True
            elif flag == 'NOCOMPRESS':
                do_compress = False
            elif flag == 'ENCRYPT':
                do_encrypt = True
            elif flag == 'EX' and i + 1 < len(args):
                i += 1
                try: expire = int(args[i])
                except ValueError: pass
            i += 1
        
        # Decode client's raw Base64 to bytes
        try:
            raw_bytes = base64.b64decode(raw_b64)
        except Exception:
            raise CommandError("ERR invalid base64 data")
        
        # Apply pipeline via SecurityManager
        if context.security:
            encoded = context.security.encode_blob(raw_bytes, compress=do_compress, encrypt=do_encrypt)
        else:
            encoded = raw_b64  # Fallback: store as-is
        
        return await context.storage.set(key, encoded, expire)

@CommandRegistry.register('GETBLOB')
class GetBlobCommand(Command):
    async def execute(self, context, *args):
        """
        GETBLOB key
        
        Retrieves and decodes blob data. Returns raw Base64 of original bytes.
        Automatically reverses compress/encrypt based on stored header byte.
        """
        if len(args) != 1: raise ValueError("ERR wrong number of arguments for 'getblob' command")
        stored = await context.storage.get(args[0])
        
        if stored is None:
            return None
        
        # Decode via SecurityManager (auto-detects compress/encrypt from header)
        if context.security:
            try:
                raw_bytes = context.security.decode_blob(stored)
                # Return as raw Base64 so client can decode to bytes
                return base64.b64encode(raw_bytes).decode('ascii')
            except Exception:
                # If it fails, return as-is (might be regular string data)
                return stored
        
        return stored

@CommandRegistry.register('SUBSCRIBE')
class SubscribeCommand(Command):
    async def execute(self, context, *args):
        if len(args) < 1: raise ValueError("ERR wrong number of arguments for 'subscribe' command")
        if not context.pubsub or not context.writer:
            raise CommandError("Pub/Sub not available")
        
        for channel in args:
            context.pubsub.subscribe(channel, context.writer)
            
        # We don't return standard response here usually, we enter sub mode.
        # But for simplicity, we return "OK" or the sub message?
        # Standard Redis returns array for each channel.
        # We'll just return first one or "OK" for MVP to denote success.
        # Correct RESP: *3\r\n$9\r\nsubscribe\r\n$channel\r\n:1
        return "OK"
