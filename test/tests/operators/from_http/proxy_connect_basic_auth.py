# runner: python
# fixtures: [http_connect_proxy]
"""Verify `from_http` sends proxy credentials on CONNECT."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    pipeline = (
        f'from_http "{os.environ["HTTP_PROXY_FIXTURE_URL"]}", '
        f'tls={{cacert: "{os.environ["HTTP_PROXY_FIXTURE_CAFILE"]}"}} '
        "{ read_json }"
    )
    env = os.environ.copy()
    for key in (
        "TENZIR_HTTP_PROXY",
        "TENZIR_HTTPS_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
    ):
        env.pop(key, None)
    env["TENZIR_NO_PROXY"] = ""
    env["TENZIR_HTTPS_PROXY"] = os.environ["HTTP_PROXY_FIXTURE_PROXY"]
    command = [
        *tenzir,
        "--bare-mode",
        "--console-verbosity=error",
        pipeline,
    ]
    completed = subprocess.run(
        command,
        env=env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        failure_message = "\n".join(
            [
                "tenzir subprocess failed",
                f"command: {shlex.join(command)}",
                f"return code: {completed.returncode}",
                "stdout:",
                completed.stdout or "<empty>",
                "stderr:",
                completed.stderr or "<empty>",
            ]
        )
        if failure_log := os.environ.get("HTTP_PROXY_FIXTURE_FAILURE_LOG"):
            with open(failure_log, "w", encoding="utf-8") as handle:
                handle.write(failure_message)
        raise RuntimeError(failure_message)
    print(completed.stdout, end="")


if __name__ == "__main__":
    main()
