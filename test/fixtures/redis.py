"""Redis fixture for integration testing.

Starts a Redis server from a local `redis-server` binary. Without one, falls
back to a container, which needs Docker or Podman.

Environment variables yielded:
- REDIS_URL: URL of the server, with the `rediss` scheme for TLS.
- REDIS_ENDPOINT: `host:port` of the server.
- REDIS_CLI: Command line that runs `redis-cli` against the server, already
  authenticated as the default user. Expand it unquoted in shell tests.
- REDIS_CA: CA certificate of the server, only with TLS.
"""

from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
from tenzir_test.fixtures.container_runtime import (
    ContainerCommandError,
    ManagedContainer,
    detect_runtime,
    start_detached,
)

from ._utils import find_free_port, generate_self_signed_cert

logger = logging.getLogger(__name__)

REDIS_IMAGE = "redis:7.4-alpine"
STARTUP_TIMEOUT = 30.0
POLL_INTERVAL = 0.1


CERT_FILES = ("server-cert.pem", "server-key.pem", "ca.pem")


def _cert_variable(name: str) -> str:
    return "REDIS_" + name.removesuffix(".pem").replace("-", "_").upper()


@dataclass(frozen=True)
class RedisOptions:
    # Password of the default user.
    password: str = ""
    # An additional user with full access.
    acl_user: str = ""
    acl_password: str = ""
    tls: bool = False


def _server_args(opts: RedisOptions, port: int, cert_dir: str) -> list[str]:
    args = ["--save", "", "--appendonly", "no"]
    if opts.tls:
        args += [
            "--port",
            "0",
            "--tls-port",
            str(port),
            "--tls-cert-file",
            f"{cert_dir}/server-cert.pem",
            "--tls-key-file",
            f"{cert_dir}/server-key.pem",
            "--tls-ca-cert-file",
            f"{cert_dir}/ca.pem",
            "--tls-auth-clients",
            "no",
        ]
    else:
        args += ["--port", str(port)]
    if opts.password:
        args += ["--requirepass", opts.password]
    if opts.acl_user:
        args += [
            "--user",
            opts.acl_user,
            "on",
            f">{opts.acl_password}",
            "~*",
            "&*",
            "+@all",
        ]
    return args


def _cli_args(opts: RedisOptions, cert_dir: str) -> list[str]:
    args = []
    if opts.tls:
        args += ["--tls", "--cacert", f"{cert_dir}/ca.pem"]
    if opts.password:
        args += ["-a", opts.password, "--no-auth-warning"]
    return args


def _wait_for_ping(cli: list[str]) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    output = ""
    while time.monotonic() < deadline:
        result = subprocess.run([*cli, "ping"], capture_output=True, text=True)
        output = (result.stdout + result.stderr).strip()
        if result.returncode == 0 and output == "PONG":
            return
        time.sleep(POLL_INTERVAL)
    raise RuntimeError(f"timed out waiting for Redis startup: {output}")


@fixture(options=RedisOptions)
def redis() -> Iterator[dict[str, str]]:
    """Start a Redis server and yield environment variables."""
    opts = current_options("redis")
    if opts.acl_user and not opts.acl_password:
        raise RuntimeError("redis fixture option `acl_user` needs `acl_password`")
    server = shutil.which("redis-server")
    client = shutil.which("redis-cli")
    runtime = None if server and client else detect_runtime()
    if not (server and client) and runtime is None:
        raise FixtureUnavailable(
            "redis-server and redis-cli or a container runtime required"
        )
    port = find_free_port()
    temp_dir = Path(tempfile.mkdtemp(prefix="tenzir-redis-"))
    process: subprocess.Popen[bytes] | None = None
    container: ManagedContainer | None = None
    try:
        if opts.tls:
            _, _, ca, _ = generate_self_signed_cert(temp_dir)
        if server and client:
            cert_dir = str(temp_dir)
            process = subprocess.Popen(
                [server, *_server_args(opts, port, cert_dir), "--dir", cert_dir],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            cli = [client, "-h", "127.0.0.1", "-p", str(port)]
        else:
            assert runtime is not None
            # Pass the certificates through the environment, because the
            # runtime may not share the temporary directory with its VM.
            cert_dir = "/tmp"
            cert_env = []
            for name in CERT_FILES:
                path = temp_dir / name
                if path.exists():
                    cert_env += ["-e", f"{_cert_variable(name)}={path.read_text()}"]
            script = "".join(
                f'printf "%s" "${_cert_variable(name)}" >{cert_dir}/{name} && '
                for name in CERT_FILES
                if (temp_dir / name).exists()
            )
            try:
                container = start_detached(
                    runtime,
                    [
                        "--rm",
                        "--name",
                        f"tenzir-test-redis-{uuid.uuid4().hex[:8]}",
                        "-p",
                        f"127.0.0.1:{port}:{port}",
                        *cert_env,
                        REDIS_IMAGE,
                        "sh",
                        "-c",
                        f'{script}exec redis-server "$@"',
                        "redis-server",
                        *_server_args(opts, port, cert_dir),
                    ],
                )
            except ContainerCommandError as exc:
                raise FixtureUnavailable(
                    f"failed to start Redis container: {exc}"
                ) from exc
            cli = [
                runtime.binary,
                "exec",
                "-i",
                container.container_id,
                "redis-cli",
                "-p",
                str(port),
            ]
        cli += _cli_args(opts, cert_dir)
        _wait_for_ping(cli)
        endpoint = f"127.0.0.1:{port}"
        env = {
            "REDIS_URL": f"{'rediss' if opts.tls else 'redis'}://{endpoint}",
            "REDIS_ENDPOINT": endpoint,
            "REDIS_CLI": shlex.join(cli),
        }
        if opts.tls:
            env["REDIS_CA"] = str(ca)
        yield env
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if container is not None:
            result = container.stop()
            if result.returncode != 0:
                logger.warning(
                    "failed to stop Redis container %s",
                    container.container_id[:12],
                )
        shutil.rmtree(temp_dir, ignore_errors=True)
