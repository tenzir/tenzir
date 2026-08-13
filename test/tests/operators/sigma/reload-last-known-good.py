# runner: python
# fixtures: [local_files]
# timeout: 30

"""Verify that hot reload retains rules after a source becomes invalid."""

from __future__ import annotations

import json
import os
import select
import shlex
import shutil
import subprocess
import time
from pathlib import Path


VALID_RULE = """\
title: Valid rule
detection:
  selection:
    foo: alpha
  condition: selection
"""

INVALID_RULE = """\
title: Invalid rule
detection:
  selection:
    foo|frobnicate: alpha
  condition: selection
"""

VALID_MULTI_RULE = (
    VALID_RULE
    + """\
---
title: Second valid rule
detection:
  selection:
    bar: bravo
  condition: selection
"""
)

SHIFTED_INVALID_MULTI_RULE = (
    VALID_RULE
    + """\
---
title: Inserted invalid rule
detection:
  selection:
    x|frobnicate: charlie
  condition: selection
---
title: Second valid rule
detection:
  selection:
    bar: bravo
  condition: selection
"""
)


def resolve_tenzir_binary() -> tuple[str, ...]:
    if value := os.environ.get("TENZIR_BINARY"):
        return tuple(shlex.split(value))
    if binary := shutil.which("tenzir"):
        return (binary,)
    raise RuntimeError("tenzir executable not found")


def start_process(rule: Path) -> subprocess.Popen[str]:
    pipeline = "\n".join(
        [
            "from_stdin { read_ndjson }",
            f"sigma path={json.dumps(str(rule))}, refresh_interval=10ms",
            "select event.id",
            "write_ndjson",
        ]
    )
    return subprocess.Popen(
        [
            *resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            pipeline,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def read_event(
    process: subprocess.Popen[str], expected_id: str, diagnostics: list[str]
) -> None:
    assert process.stdout is not None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        readable, _, _ = select.select(
            [process.stdout], [], [], deadline - time.monotonic()
        )
        if not readable:
            break
        line = process.stdout.readline()
        if not line:
            break
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            diagnostics.append(line)
            continue
        actual_id = event.get("id", event.get("event", {}).get("id"))
        if actual_id != expected_id:
            raise RuntimeError(f"expected event {expected_id!r}, got {event!r}")
        return
    raise RuntimeError(f"timed out waiting for event {expected_id!r}")


def send_event(
    process: subprocess.Popen[str], event: dict[str, str], diagnostics: list[str]
) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps(event) + "\n")
    process.stdin.flush()
    read_event(process, event["id"], diagnostics)


def finish_process(process: subprocess.Popen[str], diagnostics: list[str]) -> None:
    assert process.stdin is not None
    process.stdin.close()
    process.wait(timeout=10)
    assert process.stdout is not None
    assert process.stderr is not None
    diagnostics.append(process.stdout.read())
    diagnostics.append(process.stderr.read())
    combined_diagnostics = "".join(diagnostics)
    if process.returncode != 0:
        raise RuntimeError(combined_diagnostics)
    warning = "sigma operator retains last known good version of"
    warning_count = combined_diagnostics.count(warning)
    if warning_count != 1:
        raise RuntimeError(
            f"expected one retention warning, got {warning_count}:\n"
            f"{combined_diagnostics}"
        )


def test_single_document(rule: Path) -> None:
    rule.write_text(VALID_RULE)
    process = start_process(rule)
    diagnostics: list[str] = []
    send_event(process, {"id": "before", "foo": "alpha"}, diagnostics)
    rule.write_text(INVALID_RULE)
    time.sleep(0.05)
    send_event(process, {"id": "retained", "foo": "alpha"}, diagnostics)
    time.sleep(0.05)
    send_event(process, {"id": "still-retained", "foo": "alpha"}, diagnostics)
    finish_process(process, diagnostics)


def test_empty_source(rule: Path) -> None:
    rule.write_text(VALID_RULE)
    process = start_process(rule)
    diagnostics: list[str] = []
    send_event(process, {"id": "before-empty", "foo": "alpha"}, diagnostics)
    rule.write_text("")
    time.sleep(0.05)
    send_event(process, {"id": "after-empty", "foo": "alpha"}, diagnostics)
    time.sleep(0.05)
    send_event(process, {"id": "still-after-empty", "foo": "alpha"}, diagnostics)
    finish_process(process, diagnostics)


def test_shifted_document(rule: Path) -> None:
    rule.write_text(VALID_MULTI_RULE)
    process = start_process(rule)
    diagnostics: list[str] = []
    send_event(process, {"id": "before-shift", "bar": "bravo"}, diagnostics)
    rule.write_text(SHIFTED_INVALID_MULTI_RULE)
    time.sleep(0.05)
    send_event(process, {"id": "after-shift", "bar": "bravo"}, diagnostics)
    time.sleep(0.05)
    # A duplicated retained sibling would leave another `after-shift` result in
    # stdout, causing this read to observe the wrong ID.
    send_event(process, {"id": "shift-stable", "bar": "bravo"}, diagnostics)
    finish_process(process, diagnostics)


def main() -> None:
    root = Path(os.environ["FILE_ROOT"]) / "sigma-reload"
    root.mkdir()
    rule = root / "rule.yaml"
    test_single_document(rule)
    print("single-document retention: ok")
    test_empty_source(rule)
    print("empty-source retention: ok")
    test_shifted_document(rule)
    print("shifted-document retention: ok")
    print("failure warning count: 1 per revision")


if __name__ == "__main__":
    main()
