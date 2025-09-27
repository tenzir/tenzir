"""Compatibility wrapper that re-exports asyncio helpers from tenzir-core."""

from tenzir_core.asyncio import *  # noqa: F401,F403
from tenzir_core import asyncio as _asyncio

__all__ = getattr(_asyncio, "__all__", [name for name in dir(_asyncio) if not name.startswith("_")])
