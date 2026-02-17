"""Project fixtures registered with tenzir-test."""

from __future__ import annotations

# Import modules so their decorators register fixtures on import.
from . import abs  # noqa: F401
from . import gcs  # noqa: F401
from . import kafka  # noqa: F401
from . import local_files  # noqa: F401
from . import mysql  # noqa: F401
from . import s3  # noqa: F401
from . import tcp  # noqa: F401

__all__ = ["abs", "gcs", "kafka", "local_files", "mysql", "s3", "tcp"]
