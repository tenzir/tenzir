"""Shared helpers for test fixtures."""

from __future__ import annotations

import ipaddress
import shutil
import socket
import subprocess
import threading
from pathlib import Path

_COMMON_NAME = "tenzir-node.example.org"

# Use a deterministic, process-local allocator over a non-ephemeral range.
# This avoids collisions with client source ports from the kernel's ephemeral
# pool and guarantees that this process never hands out a port twice.
_PORT_RANGE_START = 20_000
_PORT_RANGE_END = 29_999
_PORT_ALLOCATION_LOCK = threading.Lock()
_NEXT_PORT = _PORT_RANGE_START


def _is_port_available(port: int, sock_type: int) -> bool:
    with socket.socket(socket.AF_INET, sock_type) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def find_free_port(sock_type: int = socket.SOCK_STREAM) -> int:
    """Find an available localhost port from a deterministic range."""
    global _NEXT_PORT
    with _PORT_ALLOCATION_LOCK:
        while _NEXT_PORT <= _PORT_RANGE_END:
            port = _NEXT_PORT
            _NEXT_PORT += 1
            if _is_port_available(port, sock_type):
                return port
    raise RuntimeError(
        f"exhausted localhost fixture port range {_PORT_RANGE_START}-{_PORT_RANGE_END}"
    )


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
