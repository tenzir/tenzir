"""Shared helpers for test fixtures."""

from __future__ import annotations

import shutil
import socket
import subprocess
from pathlib import Path

_COMMON_NAME = "tenzir-node.example.org"


def find_free_port(sock_type: int = socket.SOCK_STREAM) -> int:
    """Find an available port on localhost."""
    with socket.socket(socket.AF_INET, sock_type) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def find_container_runtime() -> str | None:
    """Find an available container runtime (podman or docker)."""
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def generate_self_signed_cert(
    temp_dir: Path,
    common_name: str = _COMMON_NAME,
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
    ]
    subprocess.run(
        cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    shutil.copy(cert_path, ca_path)
    cert_and_key_path.write_bytes(cert_path.read_bytes() + key_path.read_bytes())
    return cert_path, key_path, ca_path, cert_and_key_path
