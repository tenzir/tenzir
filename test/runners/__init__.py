"""Project-specific runner registrations for Tenzir integration tests."""

from .ast_runner import AstRunner  # noqa: F401
from .finalize_runner import FinalizeRunner  # noqa: F401
from .instantiation_runner import InstantiationRunner  # noqa: F401
from .ir_runner import IrRunner  # noqa: F401
from .lexer_runner import LexerRunner  # noqa: F401
from .opt_runner import OptRunner  # noqa: F401
from .oldir_runner import OldIrRunner  # noqa: F401

__all__ = [
    "AstRunner",
    "FinalizeRunner",
    "InstantiationRunner",
    "IrRunner",
    "LexerRunner",
    "OptRunner",
    "OldIrRunner",
]
