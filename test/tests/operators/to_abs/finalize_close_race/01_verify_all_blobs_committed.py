# runner: python
"""Verify every partitioned ABS sink object is committed on shutdown."""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import os
import shlex
import shutil
import subprocess
import tempfile
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path


EXPECTED_PARTITIONS = 2048


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def _shared_key_headers(
    method: str,
    path: str,
    query: str = "",
) -> dict[str, str]:
    account_name = os.environ["ABS_ACCOUNT_NAME"]
    account_key = os.environ["ABS_ACCOUNT_KEY"]
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers = {
        "x-ms-date": now,
        "x-ms-version": "2021-08-06",
    }
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
    canonical_resource = f"/{account_name}/{account_name}{path}"
    if query:
        params = urllib.parse.parse_qs(query)
        for key in sorted(params):
            canonical_resource += f"\n{key}:{','.join(sorted(params[key]))}"
    string_to_sign = (
        f"{method}\n\n\n\n\n\n\n\n\n\n\n\n"
        f"{canonical_headers}"
        f"{canonical_resource}"
    )
    signature = base64.b64encode(
        hmac.new(
            base64.b64decode(account_key),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    headers["Authorization"] = f"SharedKey {account_name}:{signature}"
    return headers


def _list_blob_names(prefix: str) -> list[str]:
    endpoint_host, endpoint_port = os.environ["ABS_ENDPOINT"].split(":", 1)
    account_name = os.environ["ABS_ACCOUNT_NAME"]
    container = os.environ["ABS_CONTAINER"]
    path = f"/{container}"
    query = urllib.parse.urlencode(
        {
            "restype": "container",
            "comp": "list",
            "prefix": prefix,
        }
    )
    request_path = f"/{account_name}{path}?{query}"
    headers = _shared_key_headers("GET", path, query)
    conn = http.client.HTTPConnection(endpoint_host, int(endpoint_port), timeout=30)
    try:
        conn.request("GET", request_path, headers=headers)
        response = conn.getresponse()
        payload = response.read()
    finally:
        conn.close()
    if response.status != 200:
        raise RuntimeError(
            f"blob listing failed with status {response.status}: "
            f"{payload.decode(errors='replace')[:200]}"
        )
    root = ET.fromstring(payload)
    result: list[str] = []
    for blob in root.iter():
        if not blob.tag.endswith("Blob"):
            continue
        for child in blob:
            if child.tag.endswith("Name") and child.text:
                result.append(child.text)
                break
    return result


def _write_pipeline(path: Path) -> None:
    padding = "x" * 2048
    path.write_text(
        "\n".join(
            [
                f'from {{padding: "{padding}"}}',
                f"repeat {EXPECTED_PARTITIONS}",
                "enumerate pk",
                'to_azure_blob_storage "abfs://" + env("ABS_ACCOUNT_NAME") + "@" + env("ABS_ENDPOINT") '
                '+ "/" + env("ABS_CONTAINER") + "/race/**/part_{uuid}.json?enable_tls=false",',
                '       account_key=env("ABS_ACCOUNT_KEY"),',
                "       partition_by=[pk] {",
                "  write_ndjson",
                "}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    with tempfile.TemporaryDirectory(prefix="to-abs-finalize-close-") as tmpdir:
        pipeline = Path(tmpdir) / "race.tql"
        _write_pipeline(pipeline)
        env = os.environ.copy()
        result = subprocess.run(
            [*tenzir, "--bare-mode", "--console-verbosity=warning", "--multi", "--neo", "-f", str(pipeline)],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )
    assert result.returncode == 0, (
        f"pipeline failed unexpectedly\nstdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    )
    names = _list_blob_names("race/")
    print(f"committed_blobs={len(names)}")
    assert len(names) == EXPECTED_PARTITIONS, (
        f"expected {EXPECTED_PARTITIONS} committed blobs, got {len(names)}"
    )
    print("ok")


if __name__ == "__main__":
    main()
