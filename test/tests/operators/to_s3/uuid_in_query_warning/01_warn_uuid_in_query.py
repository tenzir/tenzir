# runner: python
"""Verify that `{uuid}` outside the path portion produces a warning."""

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
    endpoint = os.environ["MOCK_S3_ENDPOINT"]
    bucket = os.environ["MOCK_S3_BUCKET"]
    # `{uuid}` is placed in the `tls_ca_file_path` query parameter — a
    # known Arrow S3 option that accepts an arbitrary string and, with
    # `scheme=http`, has no effect on the request. The base operator
    # should warn that the placeholder will not be expanded and the
    # object should land at the literal path.
    url = (
        f"s3://{bucket}/uuid-outside/static.json"
        f"?endpoint_override={endpoint}"
        f"&scheme=http"
        f"&allow_bucket_creation=true"
        f"&tls_ca_file_path={{uuid}}"
    )
    pipeline = (
        "from {msg: \"hello\"}\n"
        f"to_s3 \"{url}\",\n"
        "  aws_iam={access_key_id: \"test\", secret_access_key: \"test\","
        " region: \"us-east-1\"} {\n"
        "  write_ndjson\n"
        "}\n"
    )
    env = os.environ.copy()
    env.setdefault("AWS_EC2_METADATA_DISABLED", "true")
    with tempfile.TemporaryDirectory(prefix="to-s3-uuid-warn-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=warning",
                "--multi",
                "--neo",
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
    assert "`{uuid}` placeholder does not appear in the object path" in combined, (
        f"expected warning not found in output:\n{combined}"
    )
    # Confirm the object landed at the literal path (no uuid expansion).
    data_dir = Path(os.environ["MOCK_S3_JOURNAL"]).parent
    obj = data_dir / bucket / "uuid-outside" / "static.json"
    assert obj.exists(), f"expected object not found at {obj}"
    print("ok")


if __name__ == "__main__":
    main()
