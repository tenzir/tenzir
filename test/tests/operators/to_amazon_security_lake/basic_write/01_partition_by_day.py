# runner: python
"""Write three events across three distinct days and verify one object per day lands in the mock S3 under the ASL-style hive path."""

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
    # The ASL URL host must start with `aws-security-data-lake`; the mock
    # treats the host as a bucket name and accepts any label, so we pick
    # one matching the prefix. `endpoint_override=` in the query string
    # redirects Arrow's S3 client to the mock on 127.0.0.1.
    bucket = "aws-security-data-lake-test"
    url = (
        f"s3://{bucket}/ext/my-source"
        f"?endpoint_override={endpoint}"
        f"&scheme=http&allow_bucket_creation=true"
    )
    pipeline = (
        "from "
        '{time: 2024-01-01T00:00:00, msg: "a"}, '
        '{time: 2024-01-02T00:00:00, msg: "b"}, '
        '{time: 2024-01-03T00:00:00, msg: "c"}\n'
        f"to_amazon_security_lake {json.dumps(url)},\n"
        '  region="us-east-1",\n'
        '  account_id="123456789012",\n'
        "  role=null"
    )
    # Disable IMDS to avoid the 7s hang on non-EC2 hosts (see SESSION.md).
    # Supply dummy creds so the default credentials chain resolves quickly
    # without hitting any AWS endpoint; the mock S3 ignores them anyway.
    env = os.environ.copy()
    env.setdefault("AWS_EC2_METADATA_DISABLED", "true")
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    env.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    with tempfile.TemporaryDirectory(prefix="asl-basic-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [*tenzir, "--bare-mode", "--multi", "-f", str(pipe_path)],
            capture_output=True,
            text=True,
            timeout=45,
            env=env,
        )
    combined = result.stdout + "\n" + result.stderr
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{combined}"
    )
    # Inspect the mock's data dir directly (the journal is only written in
    # the fixture's assert hook, which runs after the test returns).
    data_dir = Path(os.environ["MOCK_S3_JOURNAL"]).parent
    bucket_dir = data_dir / "aws-security-data-lake-test"
    keys = sorted(
        str(p.relative_to(bucket_dir))
        for p in bucket_dir.rglob("*.parquet")
        if p.is_file()
    )
    assert len(keys) == 3, f"expected 3 objects, got {len(keys)}: {keys}"
    days = {
        seg.split("=", 1)[1]
        for key in keys
        for seg in key.split("/")
        if seg.startswith("eventDay=")
    }
    assert days == {"20240101", "20240102", "20240103"}, (
        f"unexpected eventDay set: {sorted(days)} keys={keys}"
    )
    for key in keys:
        parts = key.split("/")
        assert parts[:4] == [
            "ext",
            "my-source",
            "region=us-east-1",
            "accountId=123456789012",
        ], f"bad path: {key}"
        assert parts[4].startswith("eventDay="), f"bad path: {key}"
        assert parts[5].endswith(".parquet"), f"bad path: {key}"
    print("ok")


if __name__ == "__main__":
    main()
