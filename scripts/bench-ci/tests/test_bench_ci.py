# ruff: noqa: E402, S101

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from find_build_run import infer_event_for_ref
from parse_command import parse_command
from resolve_baselines import choose_latest_stable_release
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


def test_infer_event_for_ref_prefers_main_and_release_tags() -> None:
    assert infer_event_for_ref("main") == "push"
    assert infer_event_for_ref("v5.30.0") == "release"
    assert infer_event_for_ref("4e5a6b7") is None
