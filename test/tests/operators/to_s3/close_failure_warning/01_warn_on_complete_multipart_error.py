# runner: python
"""Verify a multipart commit failure emits a warning and does not commit the object."""

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


def _s3_key_exists(key: str) -> bool:
    result = subprocess.run(
        [
            os.environ["S3_CONTAINER_RUNTIME"],
            "exec",
            os.environ["S3_CONTAINER_ID"],
            "awslocal",
            "s3api",
            "head-object",
            "--bucket",
            os.environ["S3_BUCKET"],
            "--key",
            key,
        ],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def _write_pipeline(path: Path) -> None:
    padding = "x" * 8192
    path.write_text(
        "\n".join(
            [
                f'from {{padding: "{padding}"}}',
                "repeat 2048",
                'to_s3 env("S3_BUCKET") + "/close-failure/output.json?endpoint_override=" '
                '+ env("S3_ENDPOINT") + "&scheme=http",',
                '      aws_iam={access_key_id: env("S3_ACCESS_KEY"), '
                'secret_access_key: env("S3_SECRET_KEY")} {',
                "  write_ndjson",
                "}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    with tempfile.TemporaryDirectory(prefix="to-s3-close-failure-") as tmpdir:
        pipeline = Path(tmpdir) / "close_failure.tql"
        _write_pipeline(pipeline)
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=warning",
                "--multi",
                "-f",
                str(pipeline),
            ],
            capture_output=True,
            text=True,
            timeout=180,
            env=os.environ.copy(),
        )
    combined_output = result.stdout + "\n" + result.stderr
    assert "failed to close output stream" in combined_output, combined_output
    exists = _s3_key_exists("close-failure/output.json")
    print(f"object_exists={exists}")
    assert not exists, "object should not exist after failed multipart completion"
    print("ok")


if __name__ == "__main__":
    main()
