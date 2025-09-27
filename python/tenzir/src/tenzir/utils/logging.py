"""Compatibility wrapper that re-exports logging helpers from tenzir-common."""

from tenzir_common.logging import *  # noqa: F401,F403
from tenzir_common import logging as _logging

__all__ = getattr(_logging, "__all__", [name for name in dir(_logging) if not name.startswith("_")])
