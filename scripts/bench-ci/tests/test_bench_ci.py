# ruff: noqa: E402, S101

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
import json
import subprocess

import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
TENZIR_BENCH_SRC = Path(__file__).resolve().parents[5] / "bench" / "src"
if TENZIR_BENCH_SRC.exists() and str(TENZIR_BENCH_SRC) not in sys.path:
    sys.path.insert(0, str(TENZIR_BENCH_SRC))

import update_pr_comment as update_pr_comment_module
import common as common_module
from find_build_run import infer_event_for_ref
import find_build_run as find_build_run_module
from parse_command import parse_command
import resolve_compare_manifest as resolve_compare_manifest_module
from resolve_baselines import choose_latest_stable_release
import run_benchmarks as run_benchmarks_module
from run_benchmarks import (
    download_reference_reports,
    filter_missing_reports,
    normalize_reports,
    publish_reports,
)
from tenzir_bench.reports import Report
from update_pr_comment import (
    COMMENT_MARKER,
    current_authenticated_login,
    select_existing_comment,
    wrap_comment_body,
)
from resolve_compare_manifest import resolve_compare_manifest, storage_prefix


def test_parse_command_uses_defaults_and_target_specific_refs() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        bench_root = Path(tmpdir) / "bench"
        benchmark_dir = bench_root / "benchmarks" / "from_kafka_route53"
        benchmark_dir.mkdir(parents=True)
        (benchmark_dir / "bench.yaml").write_text("name: example\n", encoding="utf-8")
        (bench_root / "defaults.txt").write_text("from_kafka_*\n", encoding="utf-8")

        parsed = parse_command(
            "@tenzir-bot bench refs=docker@main,static@abc1234",
            root=bench_root,
        )

    assert parsed["benchmarks"] == ["from_kafka_route53"]
    assert parsed["targets"] == ["docker", "static"]
    assert parsed["refs"] == [
        {"target": "docker", "ref": "main"},
        {"target": "static", "ref": "abc1234"},
    ]


def test_parse_command_accepts_multiline_arguments() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        bench_root = Path(tmpdir) / "bench"
        benchmark_dir = bench_root / "benchmarks" / "from_kafka_route53"
        benchmark_dir.mkdir(parents=True)
        (benchmark_dir / "bench.yaml").write_text("name: example\n", encoding="utf-8")
        (bench_root / "defaults.txt").write_text("from_kafka_*\n", encoding="utf-8")

        parsed = parse_command(
            "\n".join(
                [
                    "Looks good.",
                    "@tenzir-bot benchmark benchmarks=from_kafka_route53",
                    "targets=docker,static",
                    "refs=docker@main,static@abc1234",
                ]
            ),
            root=bench_root,
        )

    assert parsed["benchmarks"] == ["from_kafka_route53"]
    assert parsed["targets"] == ["docker", "static"]
    assert parsed["refs"] == [
        {"target": "docker", "ref": "main"},
        {"target": "static", "ref": "abc1234"},
    ]


def test_choose_latest_stable_release_skips_release_candidates() -> None:
    releases = [
        {"tag_name": "v6.0.0-rc1", "draft": False, "prerelease": False},
        {"tag_name": "v5.30.0", "draft": False, "prerelease": False},
    ]

    assert choose_latest_stable_release(releases) == "v5.30.0"


def test_update_pr_comment_helpers_use_sticky_marker() -> None:
    body = wrap_comment_body("bench results")
    assert COMMENT_MARKER in body

    existing = select_existing_comment(
        [
            {"id": 1, "body": "hello", "user": {"login": "github-actions[bot]"}},
            {"id": 2, "body": body, "user": {"login": "github-actions[bot]"}},
        ],
        author_login="github-actions[bot]",
    )

    assert existing == {"id": 2, "body": body, "user": {"login": "github-actions[bot]"}}


def test_select_existing_comment_ignores_foreign_authors() -> None:
    body = wrap_comment_body("bench results")

    existing = select_existing_comment(
        [
            {"id": 1, "body": body, "user": {"login": "alice"}},
            {"id": 2, "body": body, "user": {"login": "github-actions[bot]"}},
        ],
        author_login="github-actions[bot]",
    )

    assert existing == {"id": 2, "body": body, "user": {"login": "github-actions[bot]"}}


def test_current_authenticated_login_reads_login_from_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        update_pr_comment_module,
        "gh_api",
        lambda endpoint, **_kwargs: {"login": "github-actions[bot]"},
    )

    assert current_authenticated_login() == "github-actions[bot]"


def test_current_authenticated_login_prefers_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BENCHMARK_COMMENT_AUTHOR_LOGIN", "github-actions[bot]")
    monkeypatch.setattr(
        update_pr_comment_module,
        "gh_api",
        lambda endpoint, **_kwargs: pytest.fail(
            "gh_api should not be called when author login is configured"
        ),
    )

    assert current_authenticated_login() == "github-actions[bot]"


def test_storage_prefix_uses_semantic_ref_layout() -> None:
    assert storage_prefix("tenzir-bench-data", "runs", "main", "abc123", "docker") == (
        "s3://tenzir-bench-data/runs/refs/main/abc123/docker"
    )
    assert storage_prefix("tenzir-bench-data", "", "tag", "v5.30.0", "static") == (
        "s3://tenzir-bench-data/refs/tags/v5.30.0/static"
    )


def test_update_pr_comment_paginates_before_posting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, object | None, bool]] = []

    def fake_gh_api(
        endpoint: str,
        *,
        method: str = "GET",
        payload: dict[str, object] | None = None,
        paginate: bool = False,
    ) -> object:
        calls.append((endpoint, method, payload, paginate))
        if endpoint == "user":
            return {"login": "github-actions[bot]"}
        if method == "GET":
            return [
                {
                    "id": 1,
                    "body": "unrelated",
                    "user": {"login": "github-actions[bot]"},
                },
                {
                    "id": 2,
                    "body": wrap_comment_body("existing benchmark results"),
                    "user": {"login": "github-actions[bot]"},
                },
            ]
        return {}

    monkeypatch.setattr(update_pr_comment_module, "gh_api", fake_gh_api)

    update_pr_comment_module.update_pr_comment(
        "tenzir/tenzir", 5960, "new benchmark results"
    )

    assert calls[0] == (
        "user",
        "GET",
        None,
        False,
    )
    assert calls[1] == (
        "repos/tenzir/tenzir/issues/5960/comments?per_page=100",
        "GET",
        None,
        True,
    )
    assert calls[2][0] == "repos/tenzir/tenzir/issues/comments/2"
    assert calls[2][1] == "PATCH"
    assert calls[2][3] is False


def test_resolve_compare_manifest_moves_target_logic_out_of_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def write_metadata(name: str, *, target: str) -> Path:
        metadata = tmp_path / f"{name}.json"
        metadata.write_text(
            json.dumps(
                {
                    "target": target,
                    "kind": target,
                    "image": f"ghcr.io/tenzir/{name}:latest"
                    if target == "docker"
                    else None,
                    "version": "5.30.0",
                },
            ),
            encoding="utf-8",
        )
        return metadata

    def fake_fetch_target_metadata(
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
        if ref == "v5.29.0":
            raise RuntimeError("missing release run")
        return {
            "available": True,
            "metadata_path": str(write_metadata(f"{target}-{ref}", target=target)),
            "run_id": 11,
            "resolved_sha": "deadbeef",
        }

    def fake_materialize_build(
        metadata_path: Path,
        target: str,
        label: str,
        *,
        run_id: int,
        repo: str,
        run_temp: Path,
    ) -> Path:
        resolved = run_temp / f"{label.replace(' ', '-')}.json"
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        payload["label"] = label
        payload["path"] = f"/tmp/{label.replace(' ', '-')}/bin/tenzir"
        resolved.write_text(json.dumps(payload), encoding="utf-8")
        return resolved

    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "fetch_target_metadata",
        fake_fetch_target_metadata,
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "materialize_build",
        fake_materialize_build,
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "infer_event_for_ref",
        lambda repo, ref: "release" if ref.startswith("v") else None,
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "merge_base_sha",
        lambda repo, base, head: "mergebase123",
    )

    manifest = resolve_compare_manifest(
        repo="tenzir/tenzir",
        request={
            "benchmarks": ["from_kafka_route53"],
            "targets": ["docker"],
            "refs": [
                {"target": "docker", "ref": "v5.29.0"},
                {"target": "docker", "ref": "feature-x"},
            ],
        },
        baselines={"latest_stable_tag": "v5.30.0"},
        head_sha="cafebabe",
        run_temp=tmp_path,
        bucket="tenzir-bench-data",
        prefix="runs",
    )

    assert manifest["benchmarks"] == ["from_kafka_route53"]
    assert len(manifest["builds"]) == 4
    build_payloads = [
        json.loads(Path(path).read_text(encoding="utf-8"))
        for path in manifest["builds"]
    ]
    labels = [payload["label"] for payload in build_payloads]
    assert labels == [
        "candidate docker",
        "main docker",
        "latest stable docker",
        "docker@feature-x",
    ]
    assert build_payloads[0]["role"] == "candidate"
    assert build_payloads[0]["target"] == "docker"
    assert build_payloads[0]["implicit"] is True
    assert build_payloads[0]["ref"] == "cafebabe"
    assert build_payloads[1]["role"] == "main"
    assert build_payloads[1]["ref"] == "mergebase123"
    assert build_payloads[1]["implicit"] is True
    assert (
        build_payloads[1]["storage_prefix"]
        == "s3://tenzir-bench-data/runs/refs/main/mergebase123/docker"
    )
    assert (
        build_payloads[2]["storage_prefix"]
        == "s3://tenzir-bench-data/runs/refs/tags/v5.30.0/docker"
    )
    assert build_payloads[2]["role"] == "release"
    assert build_payloads[2]["implicit"] is True
    assert build_payloads[3]["role"] == "extra"
    assert build_payloads[3]["implicit"] is False
    assert build_payloads[3]["request_index"] == 1


def test_resolve_compare_manifest_falls_back_when_merge_base_metadata_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def write_metadata(name: str, *, target: str) -> Path:
        metadata = tmp_path / f"{name}.json"
        metadata.write_text(
            json.dumps(
                {
                    "target": target,
                    "kind": target,
                    "image": f"ghcr.io/tenzir/{name}:latest"
                    if target == "docker"
                    else None,
                    "version": "5.30.0",
                }
            ),
            encoding="utf-8",
        )
        return metadata

    def fake_fetch_target_metadata(
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
        if ref == "mergebase123":
            if allow_missing_artifact:
                return {
                    "available": False,
                    "reason": "missing merge-base artifact",
                    "resolved_sha": ref,
                    "run_id": 99,
                }
            raise RuntimeError("missing merge-base artifact")
        return {
            "available": True,
            "metadata_path": str(write_metadata(f"{target}-{ref}", target=target)),
            "run_id": 11,
            "resolved_sha": "deadbeef",
        }

    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "fetch_target_metadata",
        fake_fetch_target_metadata,
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "fetch_latest_target_metadata",
        lambda repo, branch, target, *, output_dir, event: {
            "available": True,
            "metadata_path": str(write_metadata(f"{target}-main", target=target)),
            "run_id": 12,
            "resolved_sha": "fallbackmainsha",
        },
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "materialize_build",
        lambda metadata_path, target, label, *, run_id, repo, run_temp: metadata_path,
    )
    monkeypatch.setattr(
        resolve_compare_manifest_module,
        "merge_base_sha",
        lambda repo, base, head: "mergebase123",
    )

    manifest = resolve_compare_manifest(
        repo="tenzir/tenzir",
        request={
            "benchmarks": ["from_kafka_route53"],
            "targets": ["docker"],
            "refs": [],
        },
        baselines={"latest_stable_tag": None},
        head_sha="cafebabe",
        run_temp=tmp_path,
        bucket="tenzir-bench-data",
        prefix="runs",
    )

    build_payloads = [
        json.loads(Path(path).read_text(encoding="utf-8"))
        for path in manifest["builds"]
    ]
    assert build_payloads[1]["ref"] == "fallbackmainsha"
    assert (
        build_payloads[1]["storage_prefix"]
        == "s3://tenzir-bench-data/runs/refs/main/fallbackmainsha/docker"
    )


def test_load_contexts_resolves_selected_benchmarks_from_subdirectory(
    tmp_path: Path,
) -> None:
    bench_root = tmp_path / "bench"
    benchmark_dir = bench_root / "benchmarks" / "from_kafka_route53"
    benchmark_dir.mkdir(parents=True)
    (benchmark_dir / "bench.yaml").write_text(
        """
input:
  path: route53.ndjson
output:
  format: null
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (benchmark_dir / "neo.tql").write_text(
        """
---
bench:
  id: neo
---
from file
""".strip()
        + "\n",
        encoding="utf-8",
    )

    contexts = run_benchmarks_module.load_contexts(
        executor=type(
            "Executor",
            (),
            {
                "build_info": lambda self: type(
                    "BuildInfo", (), {"version": "v1.2.3"}
                )(),
                "create_context": lambda self, definition: type(
                    "Context", (), {"definition": definition}
                )(),
            },
        )(),
        bench_root=bench_root,
        benchmarks=["from_kafka_route53"],
    )

    assert len(contexts) == 1
    assert contexts[0].definition.benchmark_id == "from_kafka_route53"
    assert contexts[0].definition.implementation_id == "neo"


def test_gh_api_slurps_paginated_json(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    class Completed:
        stdout = '[[{"id":1}],[{"id":2}]]'

    def fake_run(cmd: list[str], **kwargs: object) -> Completed:
        calls.append(cmd)
        return Completed()

    monkeypatch.setattr(common_module.subprocess, "run", fake_run)

    payload = common_module.gh_api(
        "repos/tenzir/tenzir/issues/5960/comments?per_page=100", paginate=True
    )

    assert payload == [{"id": 1}, {"id": 2}]
    assert "--paginate" in calls[0]
    assert "--slurp" in calls[0]


def test_gh_api_surfaces_stderr_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise subprocess.CalledProcessError(
            1,
            [
                "gh",
                "api",
                "repos/tenzir/tenzir/issues/5940/comments",
                "--method",
                "POST",
            ],
            stderr="gh: Validation Failed (HTTP 422)",
        )

    monkeypatch.setattr(common_module.subprocess, "run", fake_run)

    with pytest.raises(common_module.GhCommandError, match="Validation Failed"):
        common_module.gh_api(
            "repos/tenzir/tenzir/issues/5940/comments",
            method="POST",
            payload={"body": "hello"},
        )


def test_infer_event_for_ref_prefers_main_and_release_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        find_build_run_module,
        "ref_is_tag",
        lambda repo, ref: ref == "v5.30.0",
    )

    assert infer_event_for_ref("tenzir/tenzir", "main") == "push"
    assert infer_event_for_ref("tenzir/tenzir", "v5.30.0") == "release"
    assert infer_event_for_ref("tenzir/tenzir", "v5.30") is None
    assert infer_event_for_ref("tenzir/tenzir", "4e5a6b7") is None


def test_list_artifacts_paginates_across_multiple_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        find_build_run_module,
        "gh_api",
        lambda endpoint, paginate=False: [
            {"artifacts": [{"name": "artifact-a"}]},
            {"artifacts": [{"name": "artifact-b"}]},
        ],
    )

    artifacts = find_build_run_module.list_artifacts("tenzir/tenzir", 123)

    assert [artifact["name"] for artifact in artifacts] == ["artifact-a", "artifact-b"]


def test_find_latest_run_with_artifact_uses_recent_successful_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_gh_json(args: list[str]) -> object:
        assert "--branch" in args
        return [
            {
                "databaseId": 11,
                "status": "completed",
                "conclusion": "success",
                "headSha": "missing",
            },
            {
                "databaseId": 12,
                "status": "completed",
                "conclusion": "success",
                "headSha": "wanted",
            },
        ]

    def fake_list_artifacts(repo: str, run_id: int) -> list[dict[str, object]]:
        if run_id == 11:
            return [{"name": "other-artifact", "expired": False}]
        return [{"name": "benchmark-target-docker-amd64", "expired": False}]

    monkeypatch.setattr(find_build_run_module, "gh_json", fake_gh_json)
    monkeypatch.setattr(find_build_run_module, "list_artifacts", fake_list_artifacts)

    run = find_build_run_module.find_latest_run_with_artifact(
        "tenzir/tenzir",
        branch="main",
        event="push",
        artifact_name="benchmark-target-docker-amd64",
    )

    assert run["databaseId"] == 12
    assert run["headSha"] == "wanted"


def test_normalize_reports_uses_benchmark_and_implementation_ids() -> None:
    report = Report(
        path=Path("/tmp/report.json"),
        pipeline="from /tmp/input | write_json",
        benchmark_id="from_file_route53_ocsf",
        implementation_id="neo",
        target="static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
        wall_clock=1.0,
        rss_kb=1024,
        build_version="v1.0.0",
        artifact_id=None,
    )

    normalized = normalize_reports({"pipeline-hash": report})

    assert normalized == {("from_file_route53_ocsf", "neo"): report}


def test_normalize_reports_rejects_missing_implementation_id() -> None:
    report = Report(
        path=Path("/tmp/report.json"),
        pipeline="pipeline-hash",
        benchmark_id="from_file_route53_ocsf",
        implementation_id=None,
        target="static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
        wall_clock=1.0,
        rss_kb=1024,
        build_version="v1.0.0",
        artifact_id=None,
    )

    with pytest.raises(RuntimeError, match="missing implementation_id"):
        normalize_reports({"pipeline-hash": report})


def test_filter_missing_reports_uses_expected_identities() -> None:
    remote = {
        "from_file_route53_ocsf/neo": Report(
            path=Path("/tmp/report.json"),
            pipeline="pipeline-hash",
            benchmark_id="from_file_route53_ocsf",
            implementation_id="neo",
            target="static",
            hardware_key="ubuntu-latest_x86_64_unknown_4c",
            wall_clock=1.0,
            rss_kb=1024,
            build_version="v1.0.0",
            artifact_id=None,
        ),
    }

    missing = filter_missing_reports(
        expected={
            ("from_file_route53_ocsf", "neo"),
            ("from_file_route53_ocsf", "legacy"),
        },
        remote_reports=remote,
    )

    assert missing == {("from_file_route53_ocsf", "legacy")}


def test_publish_reports_uses_hardware_key_in_s3_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import run_benchmarks as run_benchmarks_module

    captured: list[tuple[dict[str, Report], str]] = []

    report_path = tmp_path / "report.json"
    report_path.write_text("{}", encoding="utf-8")
    report = Report(
        path=report_path,
        pipeline="pipeline-hash",
        benchmark_id="from_file_route53_ocsf",
        implementation_id="neo",
        target="static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
        wall_clock=1.0,
        rss_kb=1024,
        build_version="v1.0.0",
        artifact_id=None,
    )

    class FakePublisher:
        def publish_reports(
            self, reports: dict[str, Report], destination: str, *, force: bool = False
        ) -> None:
            captured.append((reports, destination))

    monkeypatch.setattr(run_benchmarks_module, "Publisher", lambda: FakePublisher())

    publish_reports(
        {"from_file_route53_ocsf/neo": report},
        destination="s3://tenzir-bench-data/runs/refs/main/abc123/static",
    )

    assert captured == [
        (
            {"from_file_route53_ocsf/neo": report},
            "s3://tenzir-bench-data/runs/refs/main/abc123/static",
        ),
    ]


def test_download_reference_reports_filters_by_hardware_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import run_benchmarks as run_benchmarks_module

    report = Report(
        path=Path("/tmp/report.json"),
        pipeline="from_file_route53_ocsf/neo",
        benchmark_id="from_file_route53_ocsf",
        implementation_id="neo",
        target="static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
        wall_clock=1.0,
        rss_kb=1024,
        build_version="v1.0.0",
        artifact_id=None,
    )

    monkeypatch.setattr(
        run_benchmarks_module,
        "load_reference_reports",
        lambda destination, **kwargs: {
            ("from_file_route53_ocsf", "neo"): report,
        },
    )

    reports = download_reference_reports(
        destination="s3://tenzir-bench-data/runs/refs/main/abc123/static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
    )

    assert set(reports) == {"from_file_route53_ocsf/neo"}


def test_cmd_compare_materializes_static_only_when_reference_backfill_is_needed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    bench_root = tmp_path / "bench"
    (bench_root / "benchmarks").mkdir(parents=True)
    build_path = tmp_path / "build.json"
    build_path.write_text(
        json.dumps(
            {
                "label": "main static",
                "target": "static",
                "kind": "static",
                "storage_prefix": "s3://tenzir-bench-data/runs/refs/main/abc123/static",
                "version": "5.30.0",
                "run_id": 11,
                "artifact_name": "tenzir-static-x86_64-linux",
                "role": "main",
                "ref": "abc123",
                "implicit": True,
            }
        ),
        encoding="utf-8",
    )
    report = Report(
        path=tmp_path / "report.json",
        pipeline="pipeline",
        benchmark_id="from_file_route53_ocsf",
        implementation_id="neo",
        target="static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
        wall_clock=1.0,
        rss_kb=1024,
        build_version="5.30.0",
        artifact_id=None,
    )
    monkeypatch.setattr(
        run_benchmarks_module,
        "expected_report_identities",
        lambda paths, build, benchmark_dirs: {("from_file_route53_ocsf", "neo")},
    )
    monkeypatch.setattr(
        run_benchmarks_module,
        "download_reference_reports",
        lambda **kwargs: {"from_file_route53_ocsf/neo": report},
    )
    materialize_calls: list[bool] = []

    def fake_to_compare_build(
        paths: object, build: object, *, materialize_static: bool
    ) -> object:
        materialize_calls.append(materialize_static)
        return type("Build", (), {"label": "main static"})()

    monkeypatch.setattr(
        run_benchmarks_module, "_to_compare_build", fake_to_compare_build
    )
    monkeypatch.setattr(
        run_benchmarks_module,
        "prepare_compare_reports_for_build",
        lambda *args, **kwargs: {"pipeline": report},
    )
    monkeypatch.setattr(
        run_benchmarks_module, "render_markdown_for_builds", lambda builds: ""
    )
    monkeypatch.setattr(
        run_benchmarks_module,
        "current_hardware_key",
        lambda: "ubuntu-latest_x86_64_unknown_4c",
    )

    args = type(
        "Args",
        (),
        {
            "bench_root": str(bench_root),
            "build": [str(build_path)],
            "benchmark": None,
            "markdown_output": str(tmp_path / "comment.md"),
        },
    )()

    assert run_benchmarks_module.cmd_compare(args) == 0
    assert materialize_calls == [False]
