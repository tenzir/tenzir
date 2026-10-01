"""Mock Velociraptor gRPC API server for integration tests."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable

from ._utils import generate_self_signed_cert

_HELPER = Path(__file__).resolve().parent / "tools" / "velociraptor_server.py"

# The operator verifies the server certificate against this fixed name.
_SERVER_NAME = "VelociraptorServer"

_HOST = "127.0.0.1"
_STARTUP_TIMEOUT = 30.0
_STARTUP_POLL_INTERVAL = 0.05


@fixture(name="velociraptor")
def velociraptor() -> FixtureHandle:
    """Start a mock Velociraptor API server on a free port.

    The server and the operator authenticate each other with certificates that
    the fixture generates. See `tools/velociraptor_server.py` for the queries
    that the server answers.

    Exports:
      VELOCIRAPTOR_CONFIG - the `plugins.velociraptor` configuration for the
                            operator as JSON
    """
    for module in ("grpc", "grpc_tools"):
        if importlib.util.find_spec(module) is None:
            raise FixtureUnavailable(
                "grpcio/grpcio-tools not installed; install with: "
                "pip install grpcio grpcio-tools"
            )
    temp_dir = Path(tempfile.mkdtemp(prefix="velociraptor-"))
    try:
        server_dir = temp_dir / "server"
        client_dir = temp_dir / "client"
        server_dir.mkdir()
        client_dir.mkdir()
        try:
            server_cert, server_key, _, _ = generate_self_signed_cert(
                server_dir,
                common_name=_SERVER_NAME,
                san_entries=[f"DNS:{_SERVER_NAME}"],
            )
            client_cert, client_key, _, _ = generate_self_signed_cert(
                client_dir, common_name="tenzir", san_entries=["DNS:tenzir"]
            )
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            raise FixtureUnavailable(f"openssl unavailable: {exc}") from exc
        ready_path = temp_dir / "ready"
        log_path = temp_dir / "helper.log"
        log = log_path.open("wb")
        proc = subprocess.Popen(
            [
                sys.executable,
                str(_HELPER),
                "--server-key",
                str(server_key),
                "--server-cert",
                str(server_cert),
                "--client-cert",
                str(client_cert),
                "--ready-file",
                str(ready_path),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    def _teardown() -> None:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        log.close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    deadline = time.monotonic() + _STARTUP_TIMEOUT
    while not ready_path.exists():
        failure = None
        if proc.poll() is not None:
            failure = f"exited with code {proc.returncode}"
        elif time.monotonic() >= deadline:
            failure = "did not become ready"
        if failure is not None:
            output = log_path.read_text(errors="replace")
            _teardown()
            raise RuntimeError(f"velociraptor fixture helper {failure}: {output}")
        time.sleep(_STARTUP_POLL_INTERVAL)

    config = {
        "api_connection_string": f"{_HOST}:{ready_path.read_text().strip()}",
        "ca_certificate": server_cert.read_text(),
        "client_cert": client_cert.read_text(),
        "client_private_key": client_key.read_text(),
    }
    return FixtureHandle(
        env={"VELOCIRAPTOR_CONFIG": json.dumps(config)}, teardown=_teardown
    )
