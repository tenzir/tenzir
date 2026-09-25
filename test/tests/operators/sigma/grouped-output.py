# runner: python
# timeout: 120

"""Check grouped matching, row order, and provenance across sparse input masks."""

from __future__ import annotations

import json
import os
import shlex
import subprocess

BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
DNS_RULES = """\
title: All answers
logsource:
  category: dns_query
  product: windows
detection:
  selection:
    QueryResults|contains: needle
  condition: selection
---
title: Special answers
logsource:
  category: dns_query
  product: windows
detection:
  selection:
    QueryResults|contains: special
  condition: selection
"""
REGISTRY_RULE = """\
title: Registry variants
logsource:
  category: registry_event
  product: windows
detection:
  selection:
    TargetObject|contains: needle
  condition: selection
"""


def run(rows, rules, output_format, batch_size, predicate="keep", mapping="auto"):
    if output_format == "plain":
        output = "select event, title=rule.title"
    else:
        output = """select event=evidences[0].data, title=finding_info.title,
          value=evidences[1].sigma.fields[0].value, path=observables[0].name"""
    pipeline = f"""
from_stdin {{ read_tql _batch_size={batch_size} }}
where {predicate}
sigma rules=r#"{rules}"#, format="{output_format}", mapping="{mapping}"
{output}
write_ndjson
"""
    result = subprocess.run(
        [*BINARY, pipeline],
        input="\n".join(map(json.dumps, rows)),
        text=True,
        capture_output=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stderr
    assert not result.stderr, result.stderr
    return [json.loads(line) for line in result.stdout.splitlines()]


def finding(row, title, output_format, value, path):
    result = {"event": row, "title": title}
    if output_format == "ocsf":
        result.update(value=value, path=f"evidences[0].data.{path}")
    return result


def dns_rows(groups, repeats, mixed=False):
    result = []
    for i in range(groups * repeats):
        row = {"id": i, "keep": i % 5 != 1}
        value = f"needle-special-{i}" if i % 3 == 0 else f"needle-{i}"
        if mixed and i % 11 == 0:
            row["QueryResults"] = [value]
        else:
            # Vary only a fixed set of extension members' types to generate
            # many plan keys without a quadratic number of input columns.
            shape = i % groups
            answer = {"rdata": value}
            answer.update(
                (f"extension{bit}", 0 if shape & (1 << bit) else "x")
                for bit in range((groups - 1).bit_length())
            )
            row.update(
                class_uid=4003,
                metadata={"version": "1.9.0"},
                answers=[answer],
            )
        result.append(row)
    return result


def check_dns(rows, output_format, batch_size, mapping="auto"):
    expected = []
    for begin in range(0, len(rows), batch_size):
        batch = rows[begin : begin + batch_size]
        for title, needle in (
            ("All answers", "needle"),
            ("Special answers", "special"),
        ):
            for row in batch:
                if not row["keep"]:
                    continue
                if "QueryResults" in row:
                    value, path = row["QueryResults"], "QueryResults"
                elif mapping == "auto":
                    value, path = [row["answers"][0]["rdata"]], "answers.rdata"
                else:
                    continue
                if needle in value[0]:
                    expected.append(finding(row, title, output_format, value, path))
    actual = run(rows, DNS_RULES, output_format, batch_size, mapping=mapping)
    assert len(actual) == len(expected), (
        output_format,
        batch_size,
        len(actual),
        len(expected),
    )
    for got, want in zip(actual, expected, strict=True):
        assert got == want, (output_format, batch_size, got, want)


def check_variants(output_format):
    rows = [
        {
            "id": i,
            "keep": i % 5 != 1,
            "class_uid": 201001 if i % 2 == 0 else 201002,
            "metadata": {"version": "1.9.0"},
            "reg_key": {"path": f"needle-key-{i}"},
            "prev_reg_key": {"path": f"needle-old-{i}" if i % 3 == 0 else None},
            "reg_value": {"path": f"needle-value-{i}", "name": f"leaf-{i}"},
        }
        for i in range(12)
    ]
    expected = []
    for row in rows:
        if not row["keep"]:
            continue
        if row["class_uid"] == 201002:
            value = row["reg_value"]["path"] + "\\" + row["reg_value"]["name"]
            path = "reg_value"
        elif row["prev_reg_key"]["path"] is not None:
            value, path = row["prev_reg_key"]["path"], "prev_reg_key.path"
        else:
            value, path = row["reg_key"]["path"], "reg_key.path"
        expected.append(finding(row, "Registry variants", output_format, value, path))
    assert run(rows, REGISTRY_RULE, output_format, len(rows)) == expected


def main():
    homogeneous = dns_rows(1, 12)
    interleaved = dns_rows(128, 3, mixed=True)
    singletons = dns_rows(512, 1)
    for row in singletons:
        row["keep"] = True
    for output_format in ("plain", "ocsf"):
        # The single-group fast path must keep original physical row indices.
        check_dns(homogeneous, output_format, len(homogeneous))
        # Repeated groups have non-contiguous input rows; direct and projected
        # rows share a batch. Repeat across batch boundaries to reuse plans.
        check_dns(interleaved, output_format, len(interleaved))
        check_dns(interleaved, output_format, 17)
        # Every active row has its own plan and every row matches a broad rule.
        check_dns(singletons, output_format, len(singletons))
        check_dns(interleaved, output_format, len(interleaved), mapping="direct")
        assert run(homogeneous, DNS_RULES, output_format, 20, "false") == []
        assert run([], DNS_RULES, output_format, 20) == []
        check_variants(output_format)
    print("Grouped Sigma output preserves row order and provenance")


if __name__ == "__main__":
    main()
