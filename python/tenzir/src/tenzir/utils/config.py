"""Compatibility wrapper that re-exports config helpers from tenzir-core."""

from tenzir_core.config import *  # noqa: F401,F403
from tenzir_core import config as _config

__all__ = getattr(_config, "__all__", [name for name in dir(_config) if not name.startswith("_")])
