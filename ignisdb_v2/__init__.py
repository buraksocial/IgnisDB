"""
IgnisDB v2 - LSM-Tree + Tiered Storage Engine

High-performance key-value database with:
- Log-Structured Merge Tree storage engine
- Tiered caching (Hot LRU + Warm mmap + Cold disk)
- Fixed RAM usage (64-128 MB) regardless of data size
- Unlimited storage capacity
- Backward compatible RESP protocol server
"""

from .engine import LSMEngine, DB
from .server import Server

__version__ = "2.0.0"
__all__ = ["LSMEngine", "DB", "Server"]
