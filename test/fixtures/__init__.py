"""Project fixtures registered with tenzir-test."""

from __future__ import annotations

# Import modules so their decorators register fixtures on import.
from . import abs  # noqa: F401
from . import abs_proxy  # noqa: F401
from . import clickhouse  # noqa: F401
from . import http_request_chunked  # noqa: F401
from . import files_permission_tree  # noqa: F401
from . import ftp  # noqa: F401
from . import gcs  # noqa: F401
from . import google_cloud_logging  # noqa: F401
from . import google_secops  # noqa: F401
from . import kafka  # noqa: F401
from . import local_files  # noqa: F401
from . import mysql  # noqa: F401
from . import nats  # noqa: F401
from . import s3  # noqa: F401
from . import sentinelone  # noqa: F401
from . import splunk  # noqa: F401
from . import platform_ws  # noqa: F401
from . import http_request  # noqa: F401
from . import http_paginate  # noqa: F401
from . import http_server  # noqa: F401
from . import s3_proxy  # noqa: F401
from . import tcp  # noqa: F401
from . import udp  # noqa: F401
from . import zmq  # noqa: F401
from . import mock_s3  # noqa: F401

__all__ = [
    "abs",
    "abs_proxy",
    "clickhouse",
    "http_request_chunked",
    "files_permission_tree",
    "ftp",
    "gcs",
    "google_cloud_logging",
    "google_secops",
    "http_request",
    "http_paginate",
    "http_server",
    "kafka",
    "local_files",
    "mock_s3",
    "mysql",
    "nats",
    "platform_ws",
    "s3",
    "s3_proxy",
    "sentinelone",
    "splunk",
    "tcp",
    "udp",
    "zmq",
]
