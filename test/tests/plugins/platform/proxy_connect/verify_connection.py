# runner: python
"""Verify the platform plugin can connect through an HTTP CONNECT proxy."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path


def _resolve_node_binary() -> tuple[str, ...]:
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
    proxy_result_file = Path(os.environ["PLATFORM_WS_PROXY_RESULT_FILE"])
    with tempfile.TemporaryDirectory(prefix="platform-proxy-connect-") as tmpdir:
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
                "HTTPS_PROXY": os.environ["PLATFORM_WS_PROXY"],
                "NO_PROXY": "",
                "no_proxy": "",
                # The test only needs the platform client. Disable the
                # node-to-node listener so parallel tests don't compete for the
                # default endpoint port.
                "TENZIR_ENDPOINT": "false",
                "TENZIR_STATE_DIRECTORY": tmpdir,
                "TENZIR_CACHE_DIRECTORY": tmpdir,
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
                proxy_status = wait_for_result(proxy_result_file, proc, timeout=1.0)
                if (
                    status != "client-cert-authenticated"
                    or proxy_status != "connect-seen"
                ):
                    time.sleep(0.2)
                    proc.poll()
                    log.seek(0)
                    logs = log.read().decode("utf-8", errors="replace")
                    raise RuntimeError(
                        "expected client-cert-authenticated via proxy, "
                        f"got platform={status} proxy={proxy_status}\n{logs}"
                    )
                print("proxied-client-cert-authenticated")
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
