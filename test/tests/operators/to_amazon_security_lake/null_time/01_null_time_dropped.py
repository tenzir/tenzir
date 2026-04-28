# runner: python
"""Missing `time` field: slice is skipped with a warning; no objects are written."""

from __future__ import annotations

import json
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
    raise RuntimeError("tenzir executable not found")


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    endpoint = os.environ["MOCK_S3_ENDPOINT"]
    bucket = "aws-security-data-lake-test"
    url = (
        f"s3://{bucket}/ext/my-source"
        f"?endpoint_override={endpoint}"
        f"&scheme=http&allow_bucket_creation=true"
    )
    # No `time` field on any event — preprocess() should warn and skip.
    pipeline = (
        "from {x: 1}, {x: 2}\n"
        f"to_amazon_security_lake {json.dumps(url)},\n"
        "  region=\"us-east-1\",\n"
        "  account_id=\"123456789012\",\n"
        "  role=null"
    )
    env = os.environ.copy()
    env.setdefault("AWS_EC2_METADATA_DISABLED", "true")
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    with tempfile.TemporaryDirectory(prefix="asl-null-time-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--multi",
                "--neo",
                "--console-verbosity=warning",
                "-f",
                str(pipe_path),
            ],
            capture_output=True,
            text=True,
            timeout=45,
            env=env,
        )
    combined = result.stdout + "\n" + result.stderr
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{combined}"
    )
    assert "missing the required OCSF `time` field" in combined, (
        f"missing warning in output:\n{combined}"
    )
    data_dir = Path(os.environ["MOCK_S3_JOURNAL"]).parent
    bucket_dir = data_dir / bucket
    parquet_files = list(bucket_dir.rglob("*.parquet")) if bucket_dir.exists() else []
    assert not parquet_files, f"no objects expected, got: {parquet_files}"
    print("ok")


if __name__ == "__main__":
    main()
