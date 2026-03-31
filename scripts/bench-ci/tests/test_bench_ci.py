# ruff: noqa: E402, S101

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
TENZIR_BENCH_SRC = Path(__file__).resolve().parents[5] / "bench" / "src"
if TENZIR_BENCH_SRC.exists() and str(TENZIR_BENCH_SRC) not in sys.path:
    sys.path.insert(0, str(TENZIR_BENCH_SRC))

import update_pr_comment as update_pr_comment_module
from find_build_run import infer_event_for_ref
from parse_command import parse_command
from resolve_baselines import choose_latest_stable_release
from run_benchmarks import filter_missing_reports, normalize_reports
from tenzir_bench.reports import Report
from update_pr_comment import (
    COMMENT_MARKER,
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
            {"id": 1, "body": "hello"},
            {"id": 2, "body": body},
        ],
    )

    assert existing == {"id": 2, "body": body}


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
        if method == "GET":
            return [
                {"id": 1, "body": "unrelated"},
                {"id": 2, "body": wrap_comment_body("existing benchmark results")},
            ]
        return {}

    monkeypatch.setattr(update_pr_comment_module, "gh_api", fake_gh_api)

    update_pr_comment_module.update_pr_comment("tenzir/tenzir", 5960, "new benchmark results")

    assert calls[0] == (
        "repos/tenzir/tenzir/issues/5960/comments?per_page=100",
        "GET",
        None,
        True,
    )
    assert calls[1][0] == "repos/tenzir/tenzir/issues/comments/2"
    assert calls[1][1] == "PATCH"
    assert calls[1][3] is False


def test_infer_event_for_ref_prefers_main_and_release_tags() -> None:
    assert infer_event_for_ref("main") == "push"
    assert infer_event_for_ref("v5.30.0") == "release"
    assert infer_event_for_ref("4e5a6b7") is None


def test_normalize_reports_uses_benchmark_and_implementation_ids() -> None:
    report = Report(
        path=Path("/tmp/report.json"),
        pipeline="from /tmp/input | write_json",
        benchmark_id="from_file_route53_ocsf",
        implementation_id="neo",
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
