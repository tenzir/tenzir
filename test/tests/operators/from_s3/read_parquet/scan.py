# runner: python

"""Read Parquet through the S3 source and its byte-stream fallback."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import urllib.request


def main():
    root = Path(os.environ["READ_PUSHDOWN_ROOT"])
    endpoint = os.environ["S3_ENDPOINT"]
    bucket = os.environ["S3_PUBLIC_BUCKET"]
    key = "scan.parquet"
    url = f"http://{endpoint}/{bucket}/{key}"
    # LocalStack accepts unsigned fixture uploads. This object is separate from
    # the lifecycle objects checked by the shared S3 fixture.
    with urllib.request.urlopen(
        urllib.request.Request(url, data=(root / "input").read_bytes(), method="PUT"),
        timeout=20,
    ):
        pass
    try:
        source = f"s3://{bucket}/{key}?endpoint_override={endpoint}&scheme=http"
        for prefix in ["", "split_bytes 97\n"]:
            pipeline = (
                f"from_s3 {json.dumps(source)}, anonymous=true {{\n"
                f"{prefix}read_parquet\n}}\n"
                "where nested.x >= 54\nselect id\nhead 2\nwrite_json compact=true"
            )
            result = subprocess.run(
                [
                    *shlex.split(os.environ["TENZIR_BINARY"]),
                    "--bare-mode",
                    "--nova=true",
                    pipeline,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert result.returncode == 0, (pipeline, result.stderr)
            assert not result.stderr, result.stderr
            assert [json.loads(line) for line in result.stdout.splitlines()] == [
                {"id": 4},
                {"id": 5},
            ], result.stdout
    finally:
        with urllib.request.urlopen(
            urllib.request.Request(url, method="DELETE"), timeout=20
        ):
            pass
    print("S3 Parquet scan and byte-stream fallback agree")


if __name__ == "__main__":
    main()
