#!/usr/bin/env python3
"""Find Tenzir workflow runs and download benchmark target metadata artifacts."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any

from common import TARGET_METADATA_ARTIFACTS, gh_api, gh_json

HEX_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$", re.IGNORECASE)


class ArtifactUnavailableError(RuntimeError):
    """Raised when a workflow run cannot provide the requested artifact."""


def ref_is_tag(repo: str, ref: str) -> bool:
    try:
        gh_api(f"repos/{repo}/git/ref/tags/{ref}")
    except subprocess.CalledProcessError:
        return False
    return True


def infer_event_for_ref(repo: str, ref: str) -> str | None:
    if ref == "main":
        return "push"
    if ref.startswith("v") and ref_is_tag(repo, ref):
        return "release"
    if HEX_SHA_RE.fullmatch(ref):
        return None
    return None


def resolve_commit_sha(repo: str, ref: str) -> str:
    payload = gh_api(f"repos/{repo}/commits/{ref}")
    sha = payload.get("sha")
    if not isinstance(sha, str):
        raise RuntimeError(f"failed to resolve {ref} to a commit SHA")
    return sha


def find_run(repo: str, ref: str, *, event: str | None = None) -> dict[str, Any]:
    sha = resolve_commit_sha(repo, ref)
    expected_event = event or infer_event_for_ref(repo, ref)
    runs = gh_json(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            "tenzir.yaml",
            "--commit",
            sha,
            "--json",
            "databaseId,status,conclusion,headSha,url,displayTitle,event",
            "--limit",
            "20",
        ],
    )
    if not isinstance(runs, list) or not runs:
        raise RuntimeError(f"no Tenzir workflow run found for {sha}")
    filtered = [run for run in runs if run.get("headSha") == sha]
    if expected_event is not None:
        filtered = [run for run in filtered if run.get("event") == expected_event]
    if not filtered:
        raise RuntimeError(f"no Tenzir workflow run found for {ref} ({sha}) with event {expected_event or 'any'}")
    run = filtered[0]
    return run


def find_latest_run_with_artifact(
    repo: str,
    *,
    branch: str,
    event: str,
    artifact_name: str,
    limit: int = 50,
) -> dict[str, Any]:
    runs = gh_json(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            "tenzir.yaml",
            "--branch",
            branch,
            "--event",
            event,
            "--json",
            "databaseId,status,conclusion,headSha,url,displayTitle,event",
            "--limit",
            str(limit),
        ],
    )
    if not isinstance(runs, list) or not runs:
        raise RuntimeError(f"no Tenzir workflow runs found for branch {branch} ({event})")
    for run in runs:
        if run.get("status") != "completed" or run.get("conclusion") != "success":
            continue
        run_id = int(run["databaseId"])
        artifacts = list_artifacts(repo, run_id)
        for artifact in artifacts:
            if artifact.get("name") == artifact_name and not artifact.get("expired", False):
                return run
    raise RuntimeError(
        f"no successful Tenzir workflow run on {branch} ({event}) produced artifact {artifact_name}",
    )


def list_artifacts(repo: str, run_id: int) -> list[dict[str, Any]]:
    payload = gh_api(
        f"repos/{repo}/actions/runs/{run_id}/artifacts?per_page=100",
        paginate=True,
    )
    pages = payload if isinstance(payload, list) else [payload]
    artifacts: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict):
            raise RuntimeError("unexpected artifact response")
        page_artifacts = page.get("artifacts")
        if not isinstance(page_artifacts, list):
            raise RuntimeError("unexpected artifact response")
        artifacts.extend(page_artifacts)
    return artifacts


def wait_for_artifact(
    repo: str,
    run_id: int,
    *,
    artifact_name: str,
    timeout_seconds: int,
    interval_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        artifacts = list_artifacts(repo, run_id)
        for artifact in artifacts:
            if artifact.get("name") == artifact_name and not artifact.get("expired", False):
                return artifact
        run = gh_json(
            [
                "run",
                "view",
                str(run_id),
                "--repo",
                repo,
                "--json",
                "status,conclusion,url",
            ],
        )
        status = run.get("status")
        conclusion = run.get("conclusion")
        if status == "completed" and conclusion != "success":
            raise RuntimeError(
                f"Tenzir workflow run {run_id} completed with conclusion {conclusion}: {run.get('url')}",
            )
        if status == "completed":
            raise ArtifactUnavailableError(
                f"Tenzir workflow run {run_id} completed successfully without artifact "
                f"{artifact_name}: {run.get('url')}",
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for artifact {artifact_name} from run {run_id}")
        time.sleep(interval_seconds)


def download_artifact(repo: str, run_id: int, artifact_name: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "gh",
            "run",
            "download",
            str(run_id),
            "--repo",
            repo,
            "--name",
            artifact_name,
            "--dir",
            str(output_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    candidates = sorted(output_dir.rglob("*.json"))
    if len(candidates) != 1:
        raise RuntimeError(f"expected exactly one metadata json in {output_dir}, found {len(candidates)}")
    return candidates[0]


def fetch_target_metadata(
    repo: str,
    ref: str,
    target: str,
    *,
    output_dir: Path,
    timeout_seconds: int,
    interval_seconds: int,
    event: str | None = None,
    allow_missing_artifact: bool = False,
) -> dict[str, object]:
    artifact_name = TARGET_METADATA_ARTIFACTS[target]
    resolved_sha = resolve_commit_sha(repo, ref)
    run = find_run(repo, ref, event=event)
    run_id = int(run["databaseId"])
    try:
        _artifact = wait_for_artifact(
            repo,
            run_id,
            artifact_name=artifact_name,
            timeout_seconds=timeout_seconds,
            interval_seconds=interval_seconds,
        )
    except ArtifactUnavailableError as exc:
        if not allow_missing_artifact:
            raise
        return {
            "available": False,
            "reason": str(exc),
            "resolved_sha": resolved_sha,
            "run_id": run_id,
        }
    metadata_path = download_artifact(repo, run_id, artifact_name, output_dir)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    return {
        "available": True,
        "run_id": run_id,
        "resolved_sha": resolved_sha,
        "metadata_path": str(metadata_path.resolve()),
        "metadata": metadata,
    }


def fetch_latest_target_metadata(
    repo: str,
    branch: str,
    target: str,
    *,
    output_dir: Path,
    event: str,
) -> dict[str, object]:
    artifact_name = TARGET_METADATA_ARTIFACTS[target]
    run = find_latest_run_with_artifact(
        repo,
        branch=branch,
        event=event,
        artifact_name=artifact_name,
    )
    run_id = int(run["databaseId"])
    metadata_path = download_artifact(repo, run_id, artifact_name, output_dir)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    resolved_sha = run.get("headSha")
    if not isinstance(resolved_sha, str):
        raise RuntimeError(f"workflow run {run_id} is missing headSha")
    return {
        "available": True,
        "run_id": run_id,
        "resolved_sha": resolved_sha,
        "metadata_path": str(metadata_path.resolve()),
        "metadata": metadata,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch-target-metadata")
    fetch.add_argument("--repo", default="tenzir/tenzir", help="GitHub repository")
    fetch.add_argument("--ref", "--sha", dest="ref", required=True, help="Git ref or commit SHA to locate")
    fetch.add_argument(
        "--event",
        choices=["pull_request", "push", "release", "workflow_dispatch", "merge_group"],
        help="Preferred GitHub event for the matching Tenzir workflow run",
    )
    fetch.add_argument("--target", required=True, choices=sorted(TARGET_METADATA_ARTIFACTS))
    fetch.add_argument("--output-dir", required=True, type=Path, help="Where to download metadata")
    fetch.add_argument("--timeout-seconds", type=int, default=3600, help="Artifact wait timeout")
    fetch.add_argument("--interval-seconds", type=int, default=15, help="Artifact poll interval")
    fetch.add_argument(
        "--allow-missing-artifact",
        action="store_true",
        help="Return an unavailable result instead of failing when the run completes without the metadata artifact",
    )

    fetch_latest = subparsers.add_parser("fetch-latest-target-metadata")
    fetch_latest.add_argument("--repo", default="tenzir/tenzir", help="GitHub repository")
    fetch_latest.add_argument("--branch", required=True, help="Branch to scan for recent workflow runs")
    fetch_latest.add_argument(
        "--event",
        required=True,
        choices=["push", "workflow_dispatch", "merge_group", "pull_request", "release"],
        help="GitHub event for the matching Tenzir workflow runs",
    )
    fetch_latest.add_argument("--target", required=True, choices=sorted(TARGET_METADATA_ARTIFACTS))
    fetch_latest.add_argument("--output-dir", required=True, type=Path, help="Where to download metadata")

    args = parser.parse_args()
    if args.command == "fetch-target-metadata":
        result = fetch_target_metadata(
            args.repo,
            args.ref,
            args.target,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            interval_seconds=args.interval_seconds,
            event=args.event,
            allow_missing_artifact=args.allow_missing_artifact,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "fetch-latest-target-metadata":
        result = fetch_latest_target_metadata(
            args.repo,
            args.branch,
            args.target,
            output_dir=args.output_dir,
            event=args.event,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
