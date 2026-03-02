import time
import logging
from typing import Dict, Tuple, Any, Optional
from .exceptions import WrongTypeError

logger = logging.getLogger(__name__)

class StorageEngine:
    """
    Manages in-memory data storage and expiration.
    OPTIMIZED: Removed asyncio.Lock since we run in a single-threaded event loop
    and operations are atomic/non-blocking.
    """
    def __init__(self):
        # Data format: { key: (type, value, expiration_timestamp) }
        self._data: Dict[str, Tuple[str, Any, Optional[float]]] = {}

    def _check_and_delete_expired(self, key: str) -> bool:
        """Helper to check expiration."""
        item = self._data.get(key)
        if item:
            _, _, expiration_time = item
            if expiration_time is not None and time.time() > expiration_time:
                del self._data[key]
                return True
        return False

    async def get(self, key: str):
        if self._check_and_delete_expired(key): return None
        item = self._data.get(key)
        if item is None: return None
        type, value, _ = item
        if type != 'string': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        return value

    async def set(self, key: str, value: Any, expire_seconds: Optional[int] = None):
        expiration_time = None
        if expire_seconds is not None:
            expiration_time = time.time() + expire_seconds
        self._data[key] = ('string', value, expiration_time)
        return "OK"

    async def delete(self, key: str) -> int:
        if self._check_and_delete_expired(key): return 1
        if key in self._data:
            del self._data[key]
            return 1
        return 0

    async def expire(self, key: str, seconds: int) -> int:
        if self._check_and_delete_expired(key) or key not in self._data: return 0
        type, value, _ = self._data[key]
        self._data[key] = (type, value, time.time() + seconds)
        return 1
    
    async def lpush(self, key: str, values: list) -> int:
        self._check_and_delete_expired(key)
        item = self._data.get(key)
        if item is None:
            new_list = list(reversed(values))
            self._data[key] = ('list', new_list, None)
            return len(new_list)
        
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        current_list[:0] = reversed(values)
        return len(current_list)

    async def lrange(self, key: str, start: int, stop: int) -> list:
        if self._check_and_delete_expired(key): return []
        item = self._data.get(key)
        if item is None: return []
        
        type, current_list, _ = item
        if type != 'list': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        if stop == -1: return current_list[start:]
        return current_list[start : stop + 1]

    async def hset(self, key: str, field: str, value: str) -> int:
        self._check_and_delete_expired(key)
        item = self._data.get(key)
        if item is None:
            self._data[key] = ('hash', {field: value}, None)
            return 1
        
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        is_new = 1 if field not in current_hash else 0
        current_hash[field] = value
        return is_new

    async def hget(self, key: str, field: str):
        if self._check_and_delete_expired(key): return None
        item = self._data.get(key)
        if item is None: return None
        
        type, current_hash, _ = item
        if type != 'hash': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        return current_hash.get(field)
    
    async def sadd(self, key: str, members: list) -> int:
        self._check_and_delete_expired(key)
        item = self._data.get(key)
        if item is None:
            self._data[key] = ('set', set(), None)
            item = self._data[key]
        
        type, current_set, _ = item
        if type != 'set': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        count = 0
        for member in members:
            if member not in current_set:
                current_set.add(member)
                count += 1
        return count

    async def srem(self, key: str, members: list) -> int:
        if self._check_and_delete_expired(key): return 0
        item = self._data.get(key)
        if item is None: return 0
        
        type, current_set, _ = item
        if type != 'set': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        count = 0
        for member in members:
            if member in current_set:
                current_set.remove(member)
                count += 1
        return count

    async def smembers(self, key: str) -> list:
        if self._check_and_delete_expired(key): return []
        item = self._data.get(key)
        if item is None: return []
        
        type, current_set, _ = item
        if type != 'set': raise WrongTypeError("Operation against a key holding the wrong kind of value")
        
        return list(current_set)
    
    # --- For Persistence ---
    async def get_all_data(self):
        """Returns a copy of internal data for snapshotting."""
        # Prune expired keys first - creating list to avoid runtime error during iteration
        keys = list(self._data.keys())
        for key in keys:
            self._check_and_delete_expired(key)
        return self._data.copy()

    async def load_data(self, data):
        """Replaces internal data structure."""
        self._data = data

    async def restore_data(self, data: Dict[str, Any]) -> int:
        """Merges external data into the storage."""
        count = 0
        for key, item in data.items():
            # Item is [type, value, expire_at]
            if len(item) == 3:
                # Ensure it is stored as tuple for consistency
                self._data[key] = tuple(item)
                count += 1
        return count
