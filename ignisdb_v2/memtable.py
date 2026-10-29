"""
SkipList - Sorted in-memory data structure for MemTable

Provides O(log n) insert, delete, and search operations.
Maintains sorted order for efficient SSTable flush.
"""

import random
from typing import Optional, Iterator, Tuple, List


class SkipListNode:
    """Node in the skip list with forward pointers at each level."""
    __slots__ = ('key', 'value', 'forward')
    
    def __init__(self, key: bytes = None, value: bytes = None, level: int = 0):
        self.key = key
        self.value = value
        self.forward: List[Optional['SkipListNode']] = [None] * (level + 1)


class SkipList:
    """
    SkipList implementation for MemTable.
    
    - Maintains sorted order by key
    - O(log n) average case for insert/search/delete
    - Efficient iteration for SSTable flush
    """
    
    MAX_LEVEL = 16
    P = 0.25  # Probability factor for level generation
    
    def __init__(self):
        self.header = SkipListNode(level=self.MAX_LEVEL)
        self.level = 0
        self._size = 0
        self._bytes = 0
    
    def _random_level(self) -> int:
        """Generate a random level for new node."""
        lvl = 0
        while random.random() < self.P and lvl < self.MAX_LEVEL:
            lvl += 1
        return lvl
    
    def put(self, key: bytes, value: bytes):
        """Insert or update a key-value pair."""
        update = [None] * (self.MAX_LEVEL + 1)
        current = self.header
        
        # Find position at each level
        for i in range(self.level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current
        
        current = current.forward[0]
        
        # Update existing key
        if current is not None and current.key == key:
            old_value = current.value
            current.value = value
            self._bytes += len(value) - len(old_value)
            return
        
        # Insert new node
        new_level = self._random_level()
        if new_level > self.level:
            for i in range(self.level + 1, new_level + 1):
                update[i] = self.header
            self.level = new_level
        
        new_node = SkipListNode(key, value, new_level)
        for i in range(new_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
        
        self._size += 1
        self._bytes += len(key) + len(value)
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Get value for a key, or None if not found."""
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
        
        current = current.forward[0]
        if current is not None and current.key == key:
            return current.value
        return None
    
    def delete(self, key: bytes) -> bool:
        """Delete a key. Returns True if key was found."""
        update = [None] * (self.MAX_LEVEL + 1)
        current = self.header
        
        for i in range(self.level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current
        
        current = current.forward[0]
        
        if current is not None and current.key == key:
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]
            
            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1
            
            self._size -= 1
            self._bytes -= len(key) + len(current.value)
            return True
        return False
    
    def contains(self, key: bytes) -> bool:
        """Check if key exists."""
        return self.get(key) is not None
    
    def items(self) -> Iterator[Tuple[bytes, bytes]]:
        """Iterate over all key-value pairs in sorted order."""
        current = self.header.forward[0]
        while current is not None:
            yield current.key, current.value
            current = current.forward[0]
    
    def keys(self) -> Iterator[bytes]:
        """Iterate over all keys in sorted order."""
        current = self.header.forward[0]
        while current is not None:
            yield current.key
            current = current.forward[0]
    
    def range_iter(self, start_key: bytes = None, end_key: bytes = None) -> Iterator[Tuple[bytes, bytes]]:
        """Iterate over keys in range [start_key, end_key]."""
        current = self.header
        
        # Find starting position
        if start_key is not None:
            for i in range(self.level, -1, -1):
                while current.forward[i] is not None and current.forward[i].key < start_key:
                    current = current.forward[i]
            current = current.forward[0]
        else:
            current = current.forward[0]
        
        # Iterate until end
        while current is not None:
            if end_key is not None and current.key > end_key:
                break
            yield current.key, current.value
            current = current.forward[0]
    
    def __len__(self) -> int:
        return self._size
    
    def __contains__(self, key: bytes) -> bool:
        return self.contains(key)
    
    @property
    def size(self) -> int:
        """Approximate memory usage in bytes."""
        return self._bytes
    
    def clear(self):
        """Clear all entries."""
        self.header = SkipListNode(level=self.MAX_LEVEL)
        self.level = 0
        self._size = 0
        self._bytes = 0
