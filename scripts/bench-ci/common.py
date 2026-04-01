"""Shared helpers for benchmark CI scripts."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

COMMENT_MARKER = "<!-- tenzir-bench-comment -->"

TARGET_METADATA_ARTIFACTS = {
    "docker": "benchmark-target-docker-amd64",
    "static": "benchmark-target-static-x86_64-linux",
}

TARGET_PACKAGE_ARTIFACTS = {
    "static": "tenzir-static-x86_64-linux",
}


class GhCommandError(subprocess.CalledProcessError):
    """CalledProcessError with stderr/stdout folded into the string form."""

    def __str__(self) -> str:
        base = super().__str__()
        detail = (self.stderr or self.stdout or "").strip()
        if not detail:
            return base
        return f"{base}: {detail}"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def bench_root() -> Path:
    return repo_root() / "bench"


def gh_json(args: list[str]) -> Any:
    cmd = ["gh", *args]
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise GhCommandError(
            exc.returncode,
            exc.cmd,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc
    return json.loads(result.stdout)


def gh_api(
    endpoint: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    paginate: bool = False,
) -> Any:
    cmd = ["gh", "api", endpoint, "--method", method]
    if paginate:
        cmd.extend(["--paginate", "--slurp"])
    input_data = None
    if payload is not None:
        cmd.extend(["--input", "-"])
        input_data = json.dumps(payload)
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input=input_data,
        )
    except subprocess.CalledProcessError as exc:
        raise GhCommandError(
            exc.returncode,
            exc.cmd,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc
    if not result.stdout.strip():
        return {}
    payload = json.loads(result.stdout)
    if not paginate or not isinstance(payload, list):
        return payload
    if all(isinstance(page, list) for page in payload):
        flattened: list[Any] = []
        for page in payload:
            flattened.extend(page)
        return flattened
    return payload


def dump_json(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))
