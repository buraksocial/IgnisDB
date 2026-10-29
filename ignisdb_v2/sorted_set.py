"""
Sorted Set - Secondary index with score-based ordering

Redis ZSET compatible: members ordered by floating-point scores.
Supports range queries, pagination, and reverse ordering.
"""

import bisect
from typing import List, Tuple, Optional, Dict


class SortedSet:
    """
    Score-sorted set of unique members.
    
    Internal structure:
    - _members: dict mapping member -> score (O(1) score lookup)
    - _sorted: sorted list of (score, member) tuples for range queries
    """
    
    def __init__(self):
        self._members: Dict[bytes, float] = {}
        self._sorted: List[Tuple[float, bytes]] = []
    
    def zadd(self, score: float, member: bytes) -> int:
        """
        Add or update a member with given score.
        Returns 1 if new member added, 0 if updated.
        """
        if member in self._members:
            old_score = self._members[member]
            # Remove old entry from sorted list
            idx = bisect.bisect_left(self._sorted, (old_score, member))
            while idx < len(self._sorted) and self._sorted[idx] != (old_score, member):
                idx += 1
            if idx < len(self._sorted):
                self._sorted.pop(idx)
            
            self._members[member] = score
            bisect.insort(self._sorted, (score, member))
            return 0
        else:
            self._members[member] = score
            bisect.insort(self._sorted, (score, member))
            return 1
    
    def zrem(self, member: bytes) -> int:
        """Remove a member. Returns 1 if removed, 0 if not found."""
        if member not in self._members:
            return 0
        
        score = self._members.pop(member)
        idx = bisect.bisect_left(self._sorted, (score, member))
        while idx < len(self._sorted) and self._sorted[idx] != (score, member):
            idx += 1
        if idx < len(self._sorted):
            self._sorted.pop(idx)
        return 1
    
    def zscore(self, member: bytes) -> Optional[float]:
        """Get score of a member."""
        return self._members.get(member)
    
    def zcard(self) -> int:
        """Return number of members."""
        return len(self._members)
    
    def zrange(self, start: int, stop: int) -> List[bytes]:
        """
        Return members by rank (index), inclusive.
        Negative indices count from end (-1 = last).
        """
        n = len(self._sorted)
        if n == 0:
            return []
        
        # Normalize negative indices
        if start < 0:
            start = max(0, n + start)
        if stop < 0:
            stop = n + stop
        
        # Clamp to valid range
        start = max(0, min(start, n - 1))
        stop = max(0, min(stop, n - 1))
        
        return [member for _, member in self._sorted[start:stop + 1]]
    
    def zrevrange(self, start: int, stop: int) -> List[bytes]:
        """Return members by rank in reverse order."""
        result = self.zrange(start, stop)
        result.reverse()
        return result
    
    def zrangebyscore(self, min_score: float, max_score: float,
                      offset: int = 0, limit: int = -1) -> List[bytes]:
        """
        Return members with scores in [min_score, max_score].
        Supports pagination via offset/limit.
        """
        # Find range using binary search
        left = bisect.bisect_left(self._sorted, (min_score,))
        right = bisect.bisect_right(self._sorted, (max_score, b'\xff' * 1024))
        
        results = [member for _, member in self._sorted[left:right]]
        
        if limit > 0:
            return results[offset:offset + limit]
        return results[offset:]
    
    def zrevrangebyscore(self, max_score: float, min_score: float,
                         offset: int = 0, limit: int = -1) -> List[bytes]:
        """Return members in reverse score order within range."""
        result = self.zrangebyscore(min_score, max_score, 0, -1)
        result.reverse()
        
        if limit > 0:
            return result[offset:offset + limit]
        return result[offset:]
    
    def zcount(self, min_score: float, max_score: float) -> int:
        """Count members with scores in [min_score, max_score]."""
        left = bisect.bisect_left(self._sorted, (min_score,))
        right = bisect.bisect_right(self._sorted, (max_score, b'\xff' * 1024))
        return right - left
    
    def zrank(self, member: bytes) -> Optional[int]:
        """Get rank (0-based index) of member by score ascending."""
        if member not in self._members:
            return None
        score = self._members[member]
        idx = bisect.bisect_left(self._sorted, (score, member))
        return idx if idx < len(self._sorted) and self._sorted[idx][1] == member else None
    
    def zrevrank(self, member: bytes) -> Optional[int]:
        """Get rank by score descending."""
        rank = self.zrank(member)
        if rank is None:
            return None
        return len(self._sorted) - 1 - rank
    
    def serialize(self) -> bytes:
        """Serialize sorted set for persistence."""
        import struct
        parts = [struct.pack('<I', len(self._sorted))]
        for score, member in self._sorted:
            parts.append(struct.pack('<dI', score, len(member)))
            parts.append(member)
        return b''.join(parts)
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'SortedSet':
        """Deserialize sorted set from bytes."""
        import struct
        ss = cls()
        pos = 0
        count = struct.unpack('<I', data[pos:pos+4])[0]
        pos += 4
        
        for _ in range(count):
            score, member_len = struct.unpack('<dI', data[pos:pos+12])
            pos += 12
            member = data[pos:pos+member_len]
            pos += member_len
            ss.zadd(score, member)
        
        return ss
    
    def clear(self):
        """Remove all members."""
        self._members.clear()
        self._sorted.clear()
