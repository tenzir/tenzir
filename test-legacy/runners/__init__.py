"""Project-specific runner registrations for Tenzir integration tests."""

from .finalize_runner import FinalizeRunner  # noqa: F401
from .multi_runner import MultiRunner  # noqa: F401
from .oldir_runner import OldIrRunner  # noqa: F401

__all__ = [
    "FinalizeRunner",
    "MultiRunner",
    "OldIrRunner",
]
