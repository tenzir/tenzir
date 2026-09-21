#!/usr/bin/env python3
"""Compare catch-all insertion with valid, sparse-invalid, and alternating input.

Uses the CLICKHOUSE_* environment supplied by the integration fixture, plus
TENZIR_BINARY and optional BENCH_BASELINE_BINARY. Each run verifies stored rows
with SQL. Timing includes process startup, parsing, preparation, and insertion.
"""

import json
import os
import shlex
import statistics
import subprocess
import time
import uuid
from pathlib import Path


def query(sql):
    result = subprocess.run(
        [
            os.environ["CLICKHOUSE_CONTAINER_RUNTIME"],
            "exec",
            os.environ["CLICKHOUSE_CONTAINER_ID"],
            "clickhouse-client",
            "--password=" + os.environ["CLICKHOUSE_PASSWORD"],
            "--query",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return result.stdout.strip()


def main():
    table = "catch_all_benchmark_" + uuid.uuid4().hex
    rows = int(os.environ.get("BENCH_ROWS", "100000"))
    repeats = int(os.environ.get("BENCH_REPEATS", "3"))
    assert rows > 0 and repeats > 0, "rows and repeats must be positive"
    binaries = [("after", os.environ["TENZIR_BINARY"])]
    if baseline := os.environ.get("BENCH_BASELINE_BINARY"):
        binaries.insert(0, ("before", baseline))
    report = []
    query(
        f"CREATE TABLE {table} (id Int64, n UInt32, "
        "extra JSON COMMENT 'tenzir:catch_all') ENGINE=Memory"
    )
    try:
        for case in ("valid", "one_bad", "alternating"):
            payload = "".join(
                json.dumps(
                    {
                        "id": i,
                        "n": 4294967296
                        if (case == "one_bad" and i == rows // 2)
                        or (case == "alternating" and i % 2)
                        else i,
                        "unmapped": {"text": "benchmark"},
                    }
                )
                + "\n"
                for i in range(rows)
            )
            for version, binary in binaries:
                samples = []
                for _ in range(repeats):
                    query(f"TRUNCATE TABLE {table}")
                    query("SYSTEM FLUSH LOGS")
                    count_inserts = (
                        "SELECT count() FROM system.query_log "
                        "WHERE type='QueryFinish' AND query_kind='Insert' "
                        f"AND has(tables, 'default.{table}')"
                    )
                    previous = int(query(count_inserts))
                    program = f"""from_stdin {{ read_json }}
                    n = uint(n)
                    to_clickhouse table="{table}",
                      host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
                      password=env("CLICKHOUSE_PASSWORD"), tls=false,
                      mode="append", _jobs=1"""
                    start = time.perf_counter()
                    result = subprocess.run(
                        [*shlex.split(binary), program],
                        input=payload,
                        text=True,
                        capture_output=True,
                        timeout=180,
                    )
                    elapsed = time.perf_counter() - start
                    assert result.returncode == 0, result.stderr
                    expected_bad = (
                        0 if case == "valid" else 1 if case == "one_bad" else rows // 2
                    )
                    expected_rows = rows - expected_bad
                    dropped_sum = (
                        0
                        if case == "valid"
                        else rows // 2
                        if case == "one_bad"
                        else expected_bad * expected_bad
                    )
                    actual = query(
                        "SELECT count(), "
                        "countIf(JSONHas(toJSONString(extra), 'n')), "
                        f"sum(id) FROM {table}"
                    )
                    expected_sum = rows * (rows - 1) // 2 - dropped_sum
                    assert actual == f"{expected_rows}\t0\t{expected_sum}", actual
                    if expected_bad:
                        assert "value out of range" in result.stderr, result.stderr
                    else:
                        assert "warning:" not in result.stderr, result.stderr
                    query("SYSTEM FLUSH LOGS")
                    inserts = int(query(count_inserts)) - previous
                    samples.append({"seconds": elapsed, "inserts": inserts})
                median = statistics.median(sample["seconds"] for sample in samples)
                report.append(
                    {
                        "case": case,
                        "version": version,
                        "input_rows": rows,
                        "stored_rows": expected_rows,
                        "dropped_rows": expected_bad,
                        "samples": samples,
                        "median_seconds": median,
                        "input_events_per_second": rows / median,
                        "stored_events_per_second": expected_rows / median,
                        "inserts_per_second": statistics.median(
                            sample["inserts"] / sample["seconds"] for sample in samples
                        ),
                    }
                )
        Path(
            os.environ.get("BENCH_OUTPUT", "/tmp/clickhouse-write-benchmark.json")
        ).write_text(json.dumps(report, indent=2) + "\n")
    finally:
        query(f"DROP TABLE {table}")
    print("ok")


if __name__ == "__main__":
    main()
