"""Shared helpers for test fixtures."""

from __future__ import annotations

import ipaddress
import shutil
import socket
import subprocess
import threading
from pathlib import Path

_COMMON_NAME = "tenzir-node.example.org"

# Keep a process-local ledger of ports that this test harness already handed out.
# This avoids intra-run collisions when many fixtures race to "find" a free port.
# It does not protect against unrelated external processes binding the same port,
# but that is substantially less likely than duplicate fixture allocation.
_PORT_ALLOCATION_LOCK = threading.Lock()
_ALLOCATED_PORTS: set[int] = set()


def find_free_port(sock_type: int = socket.SOCK_STREAM) -> int:
    """Find an available port on localhost.

    This function never returns the same port twice within one Python process.
    """
    for _ in range(512):
        with socket.socket(socket.AF_INET, sock_type) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with _PORT_ALLOCATION_LOCK:
            if port in _ALLOCATED_PORTS:
                continue
            _ALLOCATED_PORTS.add(port)
            return port
    raise RuntimeError("failed to allocate a unique free localhost port")


def find_container_runtime() -> str | None:
    """Find an available container runtime (podman or docker)."""
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def generate_self_signed_cert(
    temp_dir: Path,
    common_name: str = _COMMON_NAME,
    san_entries: list[str] = ["DNS:localhost"],
) -> tuple[Path, Path, Path, Path]:
    """Generate a self-signed certificate for testing.

    Returns (cert_path, key_path, ca_path, cert_and_key_path).
    """
    key_path = temp_dir / "server-key.pem"
    cert_path = temp_dir / "server-cert.pem"
    ca_path = temp_dir / "ca.pem"
    cert_and_key_path = temp_dir / "server-cert-and-key.pem"
    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        str(key_path),
        "-out",
        str(cert_path),
        "-days",
        "1",
        "-nodes",
        "-subj",
        f"/CN={common_name}",
        "-addext",
        f"subjectAltName={','.join(san_entries)}",
    ]
    subprocess.run(
        cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    shutil.copy(cert_path, ca_path)
    cert_and_key_path.write_bytes(cert_path.read_bytes() + key_path.read_bytes())
    return cert_path, key_path, ca_path, cert_and_key_path
