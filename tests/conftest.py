"""Pytest shared test configuration and fixtures."""

import sys
import os

# Ensure package is resolvable
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
