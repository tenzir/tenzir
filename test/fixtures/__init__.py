"""Project fixtures registered with tenzir-test."""

from __future__ import annotations

# Import modules so their decorators register fixtures on import.
from . import abs  # noqa: F401
from . import clickhouse  # noqa: F401
from . import gcs  # noqa: F401
from . import google_cloud_logging  # noqa: F401
from . import google_secops  # noqa: F401
from . import kafka  # noqa: F401
from . import local_files  # noqa: F401
from . import mysql  # noqa: F401
from . import s3  # noqa: F401
from . import sentinelone  # noqa: F401
from . import splunk  # noqa: F401
from . import platform_ws  # noqa: F401
from . import http_request  # noqa: F401
from . import http_paginate  # noqa: F401
from . import http_server  # noqa: F401
from . import tcp  # noqa: F401
from . import udp  # noqa: F401

__all__ = [
    "abs",
    "clickhouse",
    "gcs",
    "google_cloud_logging",
    "google_secops",
    "http_request",
    "http_paginate",
    "http_server",
    "kafka",
    "local_files",
    "mysql",
    "platform_ws",
    "s3",
    "sentinelone",
    "splunk",
    "tcp",
    "udp",
]
