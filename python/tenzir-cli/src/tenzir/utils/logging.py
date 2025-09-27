"""Compatibility wrapper that re-exports logging helpers from tenzir-core."""

from tenzir_core.logging import *  # noqa: F401,F403
from tenzir_core import logging as _logging

__all__ = getattr(_logging, "__all__", [name for name in dir(_logging) if not name.startswith("_")])
