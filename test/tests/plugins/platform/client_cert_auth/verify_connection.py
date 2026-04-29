# runner: python
"""Verify the platform plugin can authenticate with a client certificate."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path


def _resolve_node_binary() -> tuple[str, ...]:
    """Resolve the tenzir-node binary, respecting TENZIR_NODE_BINARY."""
    env_val = os.environ.get("TENZIR_NODE_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir-node")
    if which_result:
        return (which_result,)
    raise RuntimeError(
        "tenzir-node executable not found (set TENZIR_NODE_BINARY or add to PATH)"
    )


def wait_for_result(
    result_file: Path, proc: subprocess.Popen[str], timeout: float
) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result_file.exists():
            value = result_file.read_text(encoding="utf-8").strip()
            if value and value != "pending":
                return value
        if proc.poll() is not None:
            return "process-exited"
        time.sleep(0.1)
    return "timeout"


def main() -> None:
    node_cmd = _resolve_node_binary()

    result_file = Path(os.environ["PLATFORM_WS_RESULT_FILE"])
    with tempfile.TemporaryDirectory(prefix="platform-client-cert-") as tmpdir:
        log_file = Path(tmpdir) / "output.log"
        env = os.environ.copy()
        env.pop("TENZIR_NODE_BINARY", None)
        for name in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
            env.pop(name, None)
        env.update(
            {
                "TENZIR_TOKEN": "tnz_abcdefgh0123456789abcdef0123456789abcdef",
                "TENZIR_PLUGINS__PLATFORM__CONTROL_ENDPOINT": os.environ[
                    "PLATFORM_WS_ENDPOINT"
                ],
                "TENZIR_PLUGINS__PLATFORM__CACERT": os.environ["PLATFORM_WS_CACERT"],
                "TENZIR_PLUGINS__PLATFORM__CERTFILE": os.environ[
                    "PLATFORM_WS_CERTFILE"
                ],
                "TENZIR_PLUGINS__PLATFORM__KEYFILE": os.environ["PLATFORM_WS_KEYFILE"],
                # Use isolated directories so parallel tests don't conflict.
                "TENZIR_STATE_DIRECTORY": tmpdir,
                "TENZIR_CACHE_DIRECTORY": tmpdir,
                # Redirect the tenzir log into stderr so it ends up in our
                # captured output rather than the default log file.
                "TENZIR_LOG_FILE": "/dev/stderr",
            }
        )
        with open(log_file, "w+b") as log:
            proc = subprocess.Popen(
                [*node_cmd],
                env=env,
                stdout=log,
                stderr=log,
            )
            try:
                status = wait_for_result(result_file, proc, timeout=30.0)
                if status != "client-cert-authenticated":
                    time.sleep(0.2)
                    proc.poll()
                    log.seek(0)
                    logs = log.read().decode("utf-8", errors="replace")
                    raise RuntimeError(
                        f"expected client-cert-authenticated, got {status}\n{logs}"
                    )
                print("client-cert-authenticated")
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)


if __name__ == "__main__":
    main()
