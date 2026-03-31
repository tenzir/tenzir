# ruff: noqa: E402, S101

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
import json

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


def test_parse_command_uses_defaults_and_target_specific_refs() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        bench_root = Path(tmpdir) / "bench"
        benchmark_dir = bench_root / "benchmarks" / "from_kafka_route53"
        benchmark_dir.mkdir(parents=True)
        (benchmark_dir / "bench.yaml").write_text("name: example\n", encoding="utf-8")
        (bench_root / "defaults.txt").write_text("from_kafka_*\n", encoding="utf-8")

        parsed = parse_command(
            "/bench refs=docker@main,static@abc1234",
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


def test_current_authenticated_login_reads_login_from_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(update_pr_comment_module, "gh_api", lambda endpoint, **_kwargs: {"login": "github-actions[bot]"})

    assert current_authenticated_login() == "github-actions[bot]"


def test_update_pr_comment_paginates_before_posting(monkeypatch: pytest.MonkeyPatch) -> None:
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
                {"id": 1, "body": "unrelated", "user": {"login": "github-actions[bot]"}},
                {"id": 2, "body": wrap_comment_body("existing benchmark results"), "user": {"login": "github-actions[bot]"}},
            ]
        return {}

    monkeypatch.setattr(update_pr_comment_module, "gh_api", fake_gh_api)

    update_pr_comment_module.update_pr_comment("tenzir/tenzir", 5960, "new benchmark results")

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


def test_load_contexts_resolves_selected_benchmarks_from_subdirectory(tmp_path: Path) -> None:
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
                "build_info": lambda self: type("BuildInfo", (), {"version": "v1.2.3"})(),
                "create_context": lambda self, definition: type("Context", (), {"definition": definition})(),
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

    payload = common_module.gh_api("repos/tenzir/tenzir/issues/5960/comments?per_page=100", paginate=True)

    assert payload == [{"id": 1}, {"id": 2}]
    assert "--paginate" in calls[0]
    assert "--slurp" in calls[0]


def test_infer_event_for_ref_prefers_main_and_release_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        find_build_run_module,
        "ref_is_tag",
        lambda repo, ref: ref == "v5.30.0",
    )

    assert infer_event_for_ref("tenzir/tenzir", "main") == "push"
    assert infer_event_for_ref("tenzir/tenzir", "v5.30.0") == "release"
    assert infer_event_for_ref("tenzir/tenzir", "v5.30") is None
    assert infer_event_for_ref("tenzir/tenzir", "4e5a6b7") is None


def test_list_artifacts_paginates_across_multiple_pages(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_find_latest_run_with_artifact_uses_recent_successful_run(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_gh_json(args: list[str]) -> object:
        assert "--branch" in args
        return [
            {"databaseId": 11, "status": "completed", "conclusion": "success", "headSha": "missing"},
            {"databaseId": 12, "status": "completed", "conclusion": "success", "headSha": "wanted"},
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


def test_publish_reports_uses_hardware_key_in_s3_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    uploaded: list[tuple[str, str, str]] = []

    class FakeS3:
        def head_object(self, *, Bucket: str, Key: str) -> object:
            error = ClientError({"Error": {"Code": "404"}}, "HeadObject")
            raise error

        def upload_file(self, filename: str, bucket: str, key: str) -> None:
            uploaded.append((filename, bucket, key))

    import run_benchmarks as run_benchmarks_module
    from botocore.exceptions import ClientError

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
    monkeypatch.setattr(run_benchmarks_module.boto3, "client", lambda _service: FakeS3())

    publish_reports(
        {"from_file_route53_ocsf/neo": report},
        destination="s3://tenzir-bench-data/runs/refs/main/abc123/static",
    )

    assert uploaded == [
        (
            str(report_path),
            "tenzir-bench-data",
            "runs/refs/main/abc123/static/ubuntu-latest_x86_64_unknown_4c/from_file_route53_ocsf/neo/report.json",
        ),
    ]


def test_download_reference_reports_filters_by_hardware_key(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "pipeline": "from_file_route53_ocsf/neo",
        "benchmark_id": "from_file_route53_ocsf",
        "implementation_id": "neo",
        "target": "static",
        "hardware": {"key": "ubuntu-latest_x86_64_unknown_4c"},
        "build": {"version": "v1.0.0"},
        "runtime": {"wall_clock": 1.0, "max_resident_set_kb": 1024},
    }
    other_payload = {
        **payload,
        "hardware": {"key": "ubicloud-standard-4-arm_aarch64_unknown_4c"},
    }

    class FakeBody:
        def __init__(self, text: str) -> None:
            self.text = text

        def read(self) -> bytes:
            return self.text.encode("utf-8")

    class FakePaginator:
        def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, object]]:
            return [
                {
                    "Contents": [
                        {"Key": f"{Prefix}/ubuntu-latest_x86_64_unknown_4c/from_file_route53_ocsf/neo/report.json"},
                        {"Key": f"{Prefix}/ubicloud-standard-4-arm_aarch64_unknown_4c/from_file_route53_ocsf/neo/report.json"},
                    ],
                },
            ]

    class FakeS3:
        def get_paginator(self, _name: str) -> FakePaginator:
            return FakePaginator()

        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            if "ubuntu-latest" in Key:
                body = json.dumps(payload)
            else:
                body = json.dumps(other_payload)
            return {"Body": FakeBody(body)}

    import run_benchmarks as run_benchmarks_module

    monkeypatch.setattr(run_benchmarks_module.boto3, "client", lambda _service: FakeS3())

    reports = download_reference_reports(
        destination="s3://tenzir-bench-data/runs/refs/main/abc123/static",
        hardware_key="ubuntu-latest_x86_64_unknown_4c",
    )

    assert set(reports) == {"from_file_route53_ocsf/neo"}
