# runner: python

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path


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
    group = os.environ["LOCALSTACK_CLOUDWATCH_LOG_GROUP"]
    stream = os.environ["LOCALSTACK_CLOUDWATCH_LOG_STREAM_WRITE"]
    large = "x" * (1_048_576 + 1)
    pipeline = (
        f'from {{message: "{large}"}}, {{message: "cw-after-large"}}\n'
        f'to_cloudwatch "{group}", "{stream}",\n'
        "  message=message,\n"
        '  aws_region="us-east-1"\n'
    )
    env = os.environ.copy()
    with tempfile.TemporaryDirectory(prefix="cloudwatch-large-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=warning",
                "--multi",
                "-f",
                str(pipe_path),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
    combined = result.stdout + "\n" + result.stderr
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{combined}"
    )
    assert "CloudWatch log event exceeds maximum payload size" in combined, (
        f"expected warning not found in output:\n{combined}"
    )
    print("large_skip_warning: True")


if __name__ == "__main__":
    main()
