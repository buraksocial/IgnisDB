"""Pytest bootstrap: make the `ignisdb` package importable from the repo root."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
