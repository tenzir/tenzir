# runner: python
# fixtures: [local_files]
# timeout: 60

"""Verify transactional hot reloads of Sigma rules and filters."""

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

FILTER_TARGET_RULE = """\
title: Filter target
id: 11111111-1111-1111-1111-111111111111
logsource:
  category: process_creation
detection:
  selection:
    Image|endswith: '\\evil.exe'
  condition: selection
"""

VALID_FILTER_RULE = """\
title: Administrator filter
logsource:
  category: process_creation
filter:
  rules: [11111111-1111-1111-1111-111111111111]
  selection:
    User|startswith: adm_
  condition: not selection
"""

BROKEN_FILTER_RULE = """\
title: Broken administrator filter
logsource:
  category: process_creation
filter:
  rules: [11111111-1111-1111-1111-111111111111]
  selection:
    User|re: '['
  condition: not selection
"""

VALID_FILTERED_RULE = FILTER_TARGET_RULE + "---\n" + VALID_FILTER_RULE
VALID_MIXED_SOURCE = VALID_RULE + "---\n" + VALID_FILTER_RULE
BROKEN_MIXED_SOURCE = """\
title: Broken sibling rule
detection:
  selection:
    foo|re: '['
  condition: selection
---
""" + VALID_FILTER_RULE.replace("adm_", "guest")

IDENTIFIED_RULE = """\
title: Identified rule
id: retained-identity
detection:
  selection:
    foo: alpha
  condition: selection
"""

BROKEN_RENAMED_RULE = """\
title: Broken renamed rule
id: rejected-identity
detection:
  selection:
    foo|re: '['
  condition: selection
"""

UNRELATED_RULE = """\
title: Unrelated rule
detection:
  selection:
    bar: bravo
  condition: selection
"""

DUPLICATE_IDENTIFIED_RULE = """\
title: Duplicate identified rule
id: retained-identity
detection:
  selection:
    foo: alpha
  condition: selection
"""


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


def write_event(process: subprocess.Popen[str], event: dict[str, str]) -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps(event) + "\n")
    process.stdin.flush()


def send_event(
    process: subprocess.Popen[str], event: dict[str, str], diagnostics: list[str]
) -> None:
    write_event(process, event)
    read_event(process, event["id"], diagnostics)


def finish_process(
    process: subprocess.Popen[str],
    diagnostics: list[str],
    expected_warning_count: int = 1,
) -> None:
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
    if warning_count != expected_warning_count:
        raise RuntimeError(
            f"expected {expected_warning_count} retention warnings, "
            f"got {warning_count}:\n{combined_diagnostics}"
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


def test_removed_filter(rule: Path) -> None:
    rule.write_text(VALID_FILTERED_RULE)
    process = start_process(rule)
    diagnostics: list[str] = []
    send_event(
        process,
        {
            "id": "unfiltered-user-before-removal",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    write_event(
        process,
        {
            "id": "filtered-before-removal",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
    )
    send_event(
        process,
        {
            "id": "barrier-before-removal",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    rule.write_text(FILTER_TARGET_RULE)
    time.sleep(0.05)
    send_event(
        process,
        {
            "id": "matched-after-removal",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
        diagnostics,
    )
    finish_process(process, diagnostics, expected_warning_count=0)


def test_initial_broken_filter(root: Path) -> None:
    sources = root / "initial-broken-filter-sources"
    sources.mkdir()
    (sources / "target.yaml").write_text(FILTER_TARGET_RULE)
    (sources / "filter.yaml").write_text(BROKEN_FILTER_RULE)
    process = start_process(sources)
    diagnostics: list[str] = []
    send_event(
        process,
        {
            "id": "matched-with-initial-broken-filter",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
        diagnostics,
    )
    finish_process(process, diagnostics, expected_warning_count=0)


def test_mixed_source_rollback(root: Path) -> None:
    sources = root / "mixed-source-rollback"
    sources.mkdir()
    (sources / "target.yaml").write_text(FILTER_TARGET_RULE)
    mixed_source = sources / "mixed.yaml"
    mixed_source.write_text(VALID_MIXED_SOURCE)
    process = start_process(sources)
    diagnostics: list[str] = []
    send_event(
        process,
        {
            "id": "guest-before-mixed-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    write_event(
        process,
        {
            "id": "administrator-before-mixed-breakage",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
    )
    send_event(
        process,
        {
            "id": "barrier-before-mixed-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    mixed_source.write_text(BROKEN_MIXED_SOURCE)
    time.sleep(0.05)
    write_event(
        process,
        {
            "id": "administrator-after-mixed-breakage",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
    )
    send_event(
        process,
        {
            "id": "guest-after-mixed-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    finish_process(process, diagnostics)


def test_retained_identity_resolution(root: Path) -> None:
    sources = root / "retained-identity-resolution"
    sources.mkdir()
    identified_rule = sources / "identified.yaml"
    identified_rule.write_text(IDENTIFIED_RULE)
    other_rule = sources / "other.yaml"
    other_rule.write_text(UNRELATED_RULE)
    process = start_process(sources)
    diagnostics: list[str] = []
    send_event(process, {"id": "before-identity-breakage", "foo": "alpha"}, diagnostics)
    identified_rule.write_text(BROKEN_RENAMED_RULE)
    time.sleep(0.05)
    send_event(process, {"id": "after-identity-breakage", "foo": "alpha"}, diagnostics)
    other_rule.write_text(DUPLICATE_IDENTIFIED_RULE)
    time.sleep(0.05)
    send_event(process, {"id": "single-retained-identity", "foo": "alpha"}, diagnostics)
    send_event(process, {"id": "identity-barrier", "bar": "bravo"}, diagnostics)
    finish_process(process, diagnostics)


def test_external_filter_refresh_on_retained_target(root: Path) -> None:
    sources = root / "external-filter-refresh"
    sources.mkdir()
    target_rule = sources / "target.yaml"
    target_rule.write_text(FILTER_TARGET_RULE)
    filter_rule = sources / "filter.yaml"
    filter_rule.write_text(VALID_FILTER_RULE)
    process = start_process(sources)
    diagnostics: list[str] = []
    send_event(
        process,
        {
            "id": "guest-before-external-refresh",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    target_rule.write_text(BROKEN_RENAMED_RULE)
    time.sleep(0.05)
    send_event(
        process,
        {
            "id": "guest-with-retained-target",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    filter_rule.write_text(VALID_FILTER_RULE.replace("adm_", "guest"))
    time.sleep(0.05)
    write_event(
        process,
        {
            "id": "guest-after-external-refresh",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
    )
    send_event(
        process,
        {
            "id": "administrator-after-external-refresh",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
        diagnostics,
    )
    finish_process(process, diagnostics)


def test_broken_filter_retention(root: Path) -> None:
    sources = root / "broken-filter-reload-sources"
    sources.mkdir()
    (sources / "target.yaml").write_text(FILTER_TARGET_RULE)
    filter_rule = sources / "filter.yaml"
    filter_rule.write_text(VALID_FILTER_RULE)
    process = start_process(sources)
    diagnostics: list[str] = []
    send_event(
        process,
        {
            "id": "unfiltered-user-before-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    write_event(
        process,
        {
            "id": "filtered-before-breakage",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
    )
    send_event(
        process,
        {
            "id": "barrier-before-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
    filter_rule.write_text(BROKEN_FILTER_RULE)
    time.sleep(0.05)
    write_event(
        process,
        {
            "id": "filtered-after-breakage",
            "Image": "c:\\evil.exe",
            "User": "adm_alice",
        },
    )
    send_event(
        process,
        {
            "id": "unfiltered-user-after-breakage",
            "Image": "c:\\evil.exe",
            "User": "guest",
        },
        diagnostics,
    )
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
    test_removed_filter(rule)
    print("removed filter: ok")
    test_initial_broken_filter(root)
    print("initial broken filter: ok")
    test_mixed_source_rollback(root)
    print("mixed-source rollback: ok")
    test_retained_identity_resolution(root)
    print("retained identity resolution: ok")
    test_external_filter_refresh_on_retained_target(root)
    print("external filter refresh on retained target: ok")
    test_broken_filter_retention(root)
    print("broken-filter retention: ok")
    print("failure warning count: 1 per revision")


if __name__ == "__main__":
    main()
