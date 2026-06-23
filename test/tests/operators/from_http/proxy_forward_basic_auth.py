# runner: python
# fixtures: [http_forward_proxy]
"""Verify cleartext `from_http` uses absolute-form proxy requests."""

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
    pipeline = f'from_http "{os.environ["HTTP_PROXY_FIXTURE_URL"]}", tls=false '
    pipeline += "{ read_json }"
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
    env["TENZIR_HTTP_PROXY"] = os.environ["HTTP_PROXY_FIXTURE_PROXY"]
    completed = subprocess.run(
        [
            *tenzir,
            "--bare-mode",
            "--console-verbosity=error",
            pipeline,
        ],
        env=env,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    print(completed.stdout, end="")


if __name__ == "__main__":
    main()
