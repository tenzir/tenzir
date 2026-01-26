"""Project fixtures registered with tenzir-test."""

from __future__ import annotations

# Import modules so their decorators register fixtures on import.
from . import http, tcp, udp  # noqa: F401

__all__ = ["http", "tcp", "udp"]
