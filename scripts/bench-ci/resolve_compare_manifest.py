#!/usr/bin/env python3
"""Resolve benchmark comparison manifests for pull request benchmark runs."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from typing import Any

from common import TARGET_PACKAGE_ARTIFACTS, dump_json, gh_api, load_json
from find_build_run import (
    fetch_latest_target_metadata,
    fetch_target_metadata,
    infer_event_for_ref,
)


def storage_prefix(
    bucket: str, prefix: str, ref_kind: str, ref_value: str, target: str
) -> str:
    stem = f"s3://{bucket}/"
    cleaned_prefix = prefix.strip("/")
    if cleaned_prefix:
        stem += f"{cleaned_prefix}/"
    if ref_kind == "main":
        return f"{stem}refs/main/{ref_value}/{target}"
    return f"{stem}refs/tags/{ref_value}/{target}"


def fetch_metadata(
    repo: str,
    ref: str,
    target: str,
    destination: Path,
    *,
    event: str | None = None,
    allow_missing: bool = False,
) -> dict[str, Any]:
    try:
        return fetch_target_metadata(
            repo,
            ref,
            target,
            output_dir=destination,
            timeout_seconds=10800,
            interval_seconds=15,
            event=event,
            allow_missing_artifact=event == "release",
        )
    except Exception as exc:
        if allow_missing:
            return {"available": False, "reason": str(exc)}
        raise


def _safe_label(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "-", label)


def merge_base_sha(repo: str, base: str, head: str) -> str:
    payload = gh_api(f"repos/{repo}/compare/{base}...{head}")
    merge_base = payload.get("merge_base_commit")
    if not isinstance(merge_base, dict):
        raise RuntimeError(f"failed to resolve merge base for {base}...{head}")
    sha = merge_base.get("sha")
    if not isinstance(sha, str) or not sha:
        raise RuntimeError(f"failed to resolve merge base SHA for {base}...{head}")
    return sha


def materialize_build(
    metadata_path: Path,
    target: str,
    label: str,
    *,
    run_id: int,
    repo: str,
    run_temp: Path,
) -> Path:
    payload = load_json(metadata_path)
    payload["label"] = label
    safe_label = _safe_label(label)
    if target == "static":
        artifact_dir = run_temp / f"artifact-{safe_label}"
        subprocess.run(
            [
                "gh",
                "run",
                "download",
                str(run_id),
                "--repo",
                repo,
                "--name",
                TARGET_PACKAGE_ARTIFACTS["static"],
                "--dir",
                str(artifact_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        tarballs = sorted(artifact_dir.rglob("*.tar.gz"))
        if not tarballs:
            raise RuntimeError(f"no tarball found for {label}")
        extracted = run_temp / f"extract-{safe_label}"
        extracted.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["tar", "-xzf", str(tarballs[0]), "-C", str(extracted)], check=True
        )
        binaries = sorted(extracted.rglob("bin/tenzir"))
        if not binaries:
            raise RuntimeError(f"no tenzir binary found for {label}")
        payload["path"] = str(binaries[0])
    resolved = run_temp / f"{safe_label}.json"
    dump_json(payload, resolved)
    return resolved


def prepare_storage_backed_build(
    metadata_path: Path,
    label: str,
    *,
    run_id: int,
    storage: str,
    run_temp: Path,
    role: str,
    target: str,
    ref: str,
    implicit: bool,
    request_index: int | None = None,
) -> Path:
    payload = load_json(metadata_path)
    payload["label"] = label
    payload["run_id"] = run_id
    payload["storage_prefix"] = storage
    payload["role"] = role
    payload["target"] = target
    payload["ref"] = ref
    payload["implicit"] = implicit
    if request_index is not None:
        payload["request_index"] = request_index
    resolved = run_temp / f"{_safe_label(label)}.json"
    dump_json(payload, resolved)
    return resolved


def resolve_compare_manifest(
    *,
    repo: str,
    request: dict[str, Any],
    baselines: dict[str, Any],
    head_sha: str,
    run_temp: Path,
    bucket: str,
    prefix: str,
) -> dict[str, Any]:
    build_specs: list[str] = []
    stable_tag = baselines.get("latest_stable_tag")
    merge_base = merge_base_sha(repo, "main", head_sha)

    for target in request["targets"]:
        candidate = fetch_metadata(
            repo,
            head_sha,
            target,
            run_temp / f"candidate-{target}",
            event="pull_request",
        )
        candidate_path = materialize_build(
            Path(candidate["metadata_path"]),
            target,
            f"candidate {target}",
            run_id=int(candidate["run_id"]),
            repo=repo,
            run_temp=run_temp,
        )
        candidate_payload = load_json(candidate_path)
        candidate_payload["role"] = "candidate"
        candidate_payload["target"] = target
        candidate_payload["ref"] = head_sha
        candidate_payload["implicit"] = True
        dump_json(candidate_payload, candidate_path)
        build_specs.append(str(candidate_path))

        main = fetch_metadata(
            repo,
            merge_base,
            target,
            destination=run_temp / f"merge-base-main-{target}",
            event="push",
            allow_missing=True,
        )
        main_ref = merge_base
        if not main.get("available", True):
            print(
                f"Falling back to latest main baseline for {target}: {main['reason']}"
            )
            main = fetch_latest_target_metadata(
                repo,
                "main",
                target,
                output_dir=run_temp / f"main-{target}",
                event="push",
            )
            main_ref = str(main["resolved_sha"])
        main_path = prepare_storage_backed_build(
            Path(main["metadata_path"]),
            f"main {target}",
            run_id=int(main["run_id"]),
            storage=storage_prefix(bucket, prefix, "main", main_ref, target),
            run_temp=run_temp,
            role="main",
            target=target,
            ref=main_ref,
            implicit=True,
        )
        build_specs.append(str(main_path))

        if isinstance(stable_tag, str) and stable_tag:
            release = fetch_metadata(
                repo,
                stable_tag,
                target,
                run_temp / f"stable-{target}",
                event="release",
                allow_missing=True,
            )
            if release.get("available", True):
                release_path = prepare_storage_backed_build(
                    Path(release["metadata_path"]),
                    f"latest stable {target}",
                    run_id=int(release["run_id"]),
                    storage=storage_prefix(bucket, prefix, "tag", stable_tag, target),
                    run_temp=run_temp,
                    role="release",
                    target=target,
                    ref=stable_tag,
                    implicit=True,
                )
                build_specs.append(str(release_path))
            else:
                print(
                    f"Skipping latest stable baseline for {target}: {release['reason']}"
                )

    for request_index, selector in enumerate(request["refs"]):
        target = selector["target"]
        ref = selector["ref"]
        event = infer_event_for_ref(repo, ref)
        extra = fetch_metadata(
            repo,
            ref,
            target,
            run_temp / f"extra-{target}-{ref}",
            event=event,
            allow_missing=event == "release",
        )
        if not extra.get("available", True):
            print(f"Skipping {target}@{ref}: {extra['reason']}")
            continue
        if ref == "main":
            extra_path = prepare_storage_backed_build(
                Path(extra["metadata_path"]),
                f"{target}@{ref}",
                run_id=int(extra["run_id"]),
                storage=storage_prefix(
                    bucket, prefix, "main", str(extra["resolved_sha"]), target
                ),
                run_temp=run_temp,
                role="extra",
                target=target,
                ref=ref,
                implicit=False,
                request_index=request_index,
            )
        elif event == "release":
            extra_path = prepare_storage_backed_build(
                Path(extra["metadata_path"]),
                f"{target}@{ref}",
                run_id=int(extra["run_id"]),
                storage=storage_prefix(bucket, prefix, "tag", ref, target),
                run_temp=run_temp,
                role="release",
                target=target,
                ref=ref,
                implicit=False,
                request_index=request_index,
            )
        else:
            extra_path = materialize_build(
                Path(extra["metadata_path"]),
                target,
                f"{target}@{ref}",
                run_id=int(extra["run_id"]),
                repo=repo,
                run_temp=run_temp,
            )
            extra_payload = load_json(extra_path)
            extra_payload["role"] = "extra"
            extra_payload["target"] = target
            extra_payload["ref"] = ref
            extra_payload["implicit"] = False
            extra_payload["request_index"] = request_index
            dump_json(extra_payload, extra_path)
        build_specs.append(str(extra_path))

    return {
        "benchmarks": request["benchmarks"],
        "builds": build_specs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="tenzir/tenzir", help="GitHub repository")
    parser.add_argument(
        "--request-json", required=True, type=Path, help="Parsed benchmark request JSON"
    )
    parser.add_argument(
        "--baselines-json",
        required=True,
        type=Path,
        help="Resolved default baselines JSON",
    )
    parser.add_argument(
        "--head-sha-file",
        required=True,
        type=Path,
        help="File containing the PR head SHA",
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Where to write the compare manifest"
    )
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket used for benchmark references"
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional S3 key prefix used for benchmark references",
    )
    args = parser.parse_args()

    request = load_json(args.request_json)
    baselines = load_json(args.baselines_json)
    head_sha = args.head_sha_file.read_text(encoding="utf-8").strip()
    manifest = resolve_compare_manifest(
        repo=args.repo,
        request=request,
        baselines=baselines,
        head_sha=head_sha,
        run_temp=args.output.parent,
        bucket=args.bucket,
        prefix=args.prefix,
    )
    dump_json(manifest, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
