"""Compatibility wrapper that re-exports arrow utilities from tenzir-core."""

from tenzir_core.arrow import *  # noqa: F401,F403
from tenzir_core import arrow as _arrow

__all__ = getattr(_arrow, "__all__", [name for name in dir(_arrow) if not name.startswith("_")])
