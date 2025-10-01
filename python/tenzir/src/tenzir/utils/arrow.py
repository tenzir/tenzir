"""Compatibility wrapper that re-exports arrow utilities from tenzir-common."""

from tenzir_common.arrow import *  # noqa: F401,F403
from tenzir_common import arrow as _arrow

__all__ = getattr(
    _arrow, "__all__", [name for name in dir(_arrow) if not name.startswith("_")]
)
