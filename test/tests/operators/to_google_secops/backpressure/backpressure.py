from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from collections.abc import Callable
from pathlib import Path


def command(pipeline: str) -> list[str]:
    return [*shlex.split(os.environ["TENZIR_BINARY"]), pipeline]


def state() -> dict:
    with open(os.environ["GOOGLE_SECOPS_BP_STATE_FILE"]) as file:
        return json.load(file)


def wait_for(predicate: Callable[[dict], bool], timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = state()
        if predicate(current):
            return
        time.sleep(0.01)
    raise AssertionError(state())


def sink(options: str) -> str:
    return f"""
to_google_secops project="test-project",
  region="us",
  instance="test-instance",
  log_text=text,
  log_type=type,
  log_entry_time=2026-01-01T00:00:00,
  collection_time=2026-01-01T00:00:01,
  service_credentials=env("GOOGLE_SECOPS_BP_SERVICE_CREDENTIALS"),
  _ingestion_url=env("GOOGLE_SECOPS_BP_URL"),
  {options}
"""


def verify_retry_recovery(prefix: str) -> None:
    pipeline = f"""
from {{text: "a", type: "{prefix}_A"}},
     {{text: "b", type: "{prefix}_B"}}
""" + sink("parallel=2, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    attempts = retry_state["attempts"]
    assert attempts[f"{prefix}_A"] == 2, retry_state
    assert attempts[f"{prefix}_B"] == 2, retry_state
    assert retry_state["max_retry_inflight"][prefix] == 1, retry_state


def verify_probe_ownership() -> None:
    pipeline = """
from {text: "probe", type: "OVERLAP_PROBE"},
     {text: "late", type: "OVERLAP_LATE"}
""" + sink("parallel=2, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    assert retry_state["attempts"]["OVERLAP_PROBE"] == 2, retry_state
    assert retry_state["attempts"]["OVERLAP_LATE"] == 2, retry_state
    assert retry_state["max_retry_inflight"]["OVERLAP"] == 1, retry_state


def verify_retries_do_not_expire() -> None:
    pipeline = """
from {text: "retry", type: "RETRY_LONG"}
""" + sink("parallel=1, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    assert retry_state["attempts"]["RETRY_LONG"] == 13, retry_state


def verify_non_retryable_response_is_discarded() -> None:
    pipeline = """
from {text: "invalid", type: "PERMANENT"}
""" + sink("parallel=1, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    diagnostics = completed.stdout + completed.stderr
    assert "non-2xx response when sending events: 400" in diagnostics, (
        completed.stdout,
        completed.stderr,
    )
    assert "discarding request batch" in diagnostics, (
        completed.stdout,
        completed.stderr,
    )


def verify_oversized_udm_is_safe() -> None:
    pipeline = """
from {data: "x".repeat(100000)},
     {data: "x".repeat(100000)}
to_google_secops _dry_run=true,
  mode="udm_event",
  project="test-project",
  region="us",
  instance="test-instance",
  max_request_size=100000
"""
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)


def verify_dry_run_route_pressure_does_not_deadlock() -> None:
    pipeline = """
from {text: "a", type: "DRY_A"},
     {text: "b", type: "DRY_B"}
""" + sink("_dry_run=true, parallel=1, max_batch_events=2")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    requests = [
        json.loads(line)
        for line in completed.stdout.splitlines()
        if line.startswith('{"parent"')
    ]
    assert len(requests) == 2, (completed.stdout, completed.stderr)
    assert {request["parent"].rsplit("/", 1)[-1] for request in requests} == {
        "DRY_A",
        "DRY_B",
    }, requests


def verify_oversized_event_does_not_evict_partial_batch() -> None:
    pipeline = """
from {text: "a", type: "KEEP"},
     {text: "x".repeat(74911), type: "TOO_BIG"},
     {text: "b", type: "KEEP"}
""" + sink("_dry_run=true, parallel=1, max_request_size=100000, max_batch_events=2")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    requests = [
        json.loads(line)
        for line in completed.stdout.splitlines()
        if line.startswith('{"parent"')
    ]
    assert len(requests) == 1, (completed.stdout, completed.stderr)
    logs = requests[0]["body"]["inlineSource"]["logs"]
    assert [log["data"] for log in logs] == ["YQ==", "Yg=="], requests


def verify_late_retry_after_is_preserved() -> None:
    pipeline = """
from {text: "probe", type: "PRESERVE_PROBE"},
     {text: "late", type: "PRESERVE_LATE"}
""" + sink("parallel=2, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    late_at = retry_state["preserve_late_at"]
    probe_at = retry_state["preserve_probe_third_at"]
    assert isinstance(late_at, float), retry_state
    assert isinstance(probe_at, float), retry_state
    assert probe_at - late_at >= 1.7, retry_state


def verify_non_retryable_probe_keeps_retries_serialized() -> None:
    pipeline = """
from {text: "probe", type: "PROBE400_PROBE"},
     {text: "a", type: "PROBE400_A"},
     {text: "b", type: "PROBE400_B"}
""" + sink("parallel=3, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    assert retry_state["attempts"]["PROBE400_PROBE"] == 2, retry_state
    assert retry_state["attempts"]["PROBE400_A"] == 2, retry_state
    assert retry_state["attempts"]["PROBE400_B"] == 2, retry_state
    assert retry_state["max_retry_inflight"]["PROBE400"] == 1, retry_state


def verify_late_success_opens_gate() -> None:
    pipeline = """
from {text: "probe", type: "LATE_SUCCESS_PROBE"},
     {text: "late", type: "LATE_SUCCESS_LATE"},
     {text: "queued", type: "LATE_SUCCESS_QUEUED"}
""" + sink("parallel=3, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    assert retry_state["attempts"]["LATE_SUCCESS_PROBE"] == 2, retry_state
    assert retry_state["attempts"]["LATE_SUCCESS_LATE"] == 1, retry_state
    assert retry_state["attempts"]["LATE_SUCCESS_QUEUED"] == 2, retry_state
    assert retry_state["late_success_unblocked_before_probe"], retry_state


def verify_request_batches_respect_event_limit() -> None:
    release = Path(os.environ["GOOGLE_SECOPS_BP_RELEASE_FILE"])
    release.unlink(missing_ok=True)
    pipeline = """
from {text: "a", type: "EVENT_BOUND"},
     {text: "b", type: "EVENT_BOUND"},
     {text: "c", type: "EVENT_BOUND"},
     {text: "d", type: "EVENT_BOUND"},
     {text: "e", type: "EVENT_BOUND"}
""" + sink("parallel=2, max_batch_events=2")
    process = subprocess.Popen(
        command(pipeline),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout = ""
    stderr = ""
    batches = []
    try:
        wait_for(lambda current: len(current["event_bound_batches"]) == 2)
        time.sleep(0.25)
        batches = list(state()["event_bound_batches"])
        assert process.poll() is None
    finally:
        release.touch()
        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            stdout, stderr = process.communicate(timeout=10)
    assert batches == [2, 2], (stdout, stderr, state())
    assert state()["event_bound_batches"] == [2, 2, 1], state()
    assert process.returncode == 0, (stdout, stderr)


def verify_capacity_flushes_only_one_partial_batch() -> None:
    release = Path(os.environ["GOOGLE_SECOPS_BP_RELEASE_FILE"])
    release.unlink(missing_ok=True)
    pipeline = """
from {text: "a", type: "PROGRESS_A"},
     {text: "b", type: "PROGRESS_B"},
     {text: "c", type: "PROGRESS_C"}
""" + sink("parallel=2, max_batch_events=2, batch_timeout=10s")
    process = subprocess.Popen(
        command(pipeline),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout = ""
    stderr = ""
    arrivals = 0
    try:
        wait_for(lambda current: current["progressive_arrivals"] >= 1)
        time.sleep(0.25)
        arrivals = int(state()["progressive_arrivals"])
    finally:
        release.touch()
        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.terminate()
            stdout, stderr = process.communicate(timeout=10)
    assert arrivals == 1, (stdout, stderr, state())
    assert state()["progressive_order"][0] == "PROGRESS_A", state()
    assert state()["progressive_arrivals"] == 3, state()
    assert process.returncode == 0, (stdout, stderr)


def verify_retry_probes_are_fair() -> None:
    pipeline = """
from {text: "stuck", type: "FAIRNESS_STUCK"},
     {text: "other", type: "FAIRNESS_OTHER"}
""" + sink("parallel=2, max_batch_events=1")
    completed = subprocess.run(
        command(pipeline),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode == 0, (completed.stdout, completed.stderr)
    retry_state = state()
    # A failing request must not monopolize the shared gate merely because it
    # occupies the lowest-numbered slot.
    assert retry_state["fairness_retry_order"] == [
        "FAIRNESS_STUCK",
        "FAIRNESS_OTHER",
        "FAIRNESS_STUCK",
    ], retry_state
    assert retry_state["attempts"]["FAIRNESS_STUCK"] == 3, retry_state
    assert retry_state["attempts"]["FAIRNESS_OTHER"] == 2, retry_state


def verify_review_regressions() -> None:
    failures = []
    for check in (
        verify_dry_run_route_pressure_does_not_deadlock,
        verify_oversized_event_does_not_evict_partial_batch,
        verify_late_retry_after_is_preserved,
        verify_non_retryable_probe_keeps_retries_serialized,
        verify_late_success_opens_gate,
        verify_request_batches_respect_event_limit,
        verify_capacity_flushes_only_one_partial_batch,
        verify_retry_probes_are_fair,
    ):
        try:
            check()
        except Exception as error:
            failures.append(f"{check.__name__}: {error!r}")
    assert not failures, "\n".join(failures)


def main() -> None:
    pipeline = """
from {text: "x".repeat(60000), type: "BACKPRESSURE_Z"},
     {text: "x".repeat(60000), type: "BACKPRESSURE_Y"},
     {text: "x".repeat(60000), type: "BACKPRESSURE_X"},
     {text: "x".repeat(60000), type: "BACKPRESSURE_W"}
""" + sink("parallel=2, max_request_size=100000, max_batch_events=1")
    process = subprocess.Popen(
        command(pipeline),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(lambda current: current["arrivals"] == 2)
        assert process.poll() is None
        time.sleep(0.25)
        blocked_state = state()
        assert blocked_state["arrivals"] == 2, blocked_state
        assert blocked_state["blocked"] == 2, blocked_state
        # The two global request slots select Z and Y for capacity flushes.
        # Their concurrent HTTP arrivals may reorder.
        assert set(blocked_state["arrival_order"]) == {
            "BACKPRESSURE_Z",
            "BACKPRESSURE_Y",
        }, blocked_state
        Path(os.environ["GOOGLE_SECOPS_BP_RELEASE_FILE"]).touch()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()
    completed_state = state()
    assert completed_state["arrivals"] == 4, completed_state
    assert completed_state["max_blocked"] == 2, completed_state
    verify_retry_recovery("RETRY")
    verify_retry_recovery("RETRY429")
    verify_probe_ownership()
    verify_retries_do_not_expire()
    verify_non_retryable_response_is_discarded()
    verify_oversized_udm_is_safe()
    verify_review_regressions()
    print("bounded_inflight: true")
    print("bounded_total_request_slots: true")
    print("request_batches_respect_event_limit: true")
    print("upstream_blocked: true")
    print("successful_5xx_retry_opens_gate: true")
    print("successful_429_retry_opens_gate: true")
    print("single_probe_during_late_failure: true")
    print("retryable_failures_do_not_drop: true")
    print("non_retryable_response_warns_and_discards: true")
    print("oversized_udm_safe: true")
    print("dry_run_route_pressure_liveness: true")
    print("oversized_event_does_not_evict_partial_batch: true")
    print("late_retry_after_preserved: true")
    print("non_retryable_probe_keeps_retries_serialized: true")
    print("late_success_opens_gate: true")
    print("oldest_partial_batch_flushed_for_capacity: true")
    print("retry_probes_are_fair: true")


if __name__ == "__main__":
    main()
