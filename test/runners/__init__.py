"""Project-specific runner registrations for neo executor tests."""

from .neo import NeoRunner  # noqa: F401
from .opt_runner import OptRunner  # noqa: F401

__all__ = ["NeoRunner", "OptRunner"]
