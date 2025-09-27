"""Compatibility wrapper around the standalone tenzir-operators package."""

from tenzir_operator.executor import *  # noqa: F401,F403
from tenzir_operator import executor as _executor

__all__ = getattr(
    _executor,
    "__all__",
    [name for name in dir(_executor) if not name.startswith("_")],
)
