"""Project fixtures registered with tenzir-test."""

from __future__ import annotations

# Import modules so their decorators register fixtures on import.
from . import kafka  # noqa: F401
from . import mysql  # noqa: F401
from . import tcp  # noqa: F401

__all__ = ["kafka", "mysql", "tcp"]
