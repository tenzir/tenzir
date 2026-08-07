from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
from pathlib import Path

import pytest


def _load_fixture(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    fixtures = types.ModuleType("tenzir_bench.fixtures")
    fixtures.FixtureHandle = object
    fixtures.FixtureUnavailable = RuntimeError
    fixtures.current_context = lambda: None
    fixtures.current_options = lambda _name: None
    fixtures.fixture = lambda **_kwargs: lambda function: function
    monkeypatch.setitem(sys.modules, "tenzir_bench.fixtures", fixtures)
    spec = importlib.util.spec_from_file_location(
        "benchmark_kafka_fixture",
        Path(__file__).resolve().parents[3] / "bench/fixtures/kafka.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def test_readiness_failure_includes_compose_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _load_fixture(monkeypatch)
    calls: list[list[str]] = []

    def fake_run(
        command: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        output = "redpanda exited with status 1" if command[-1] == "redpanda" else ""
        return subprocess.CompletedProcess(command, 1, stdout=output)

    monotonic = iter((0.0, 121.0))
    monkeypatch.setattr(fixture, "_run", fake_run)
    monkeypatch.setattr(fixture.time, "monotonic", lambda: next(monotonic))

    with pytest.raises(RuntimeError, match="docker compose logs redpanda") as error:
        fixture._wait_for_cluster(
            ["docker", "compose"],
            cwd=Path.cwd(),
            service="redpanda",
            timeout_seconds=120,
            poll_interval_seconds=1,
        )

    assert "redpanda exited with status 1" in str(error.value)
    assert calls == [
        ["docker", "compose", "ps"],
        ["docker", "compose", "logs", "--no-color", "redpanda"],
    ]
