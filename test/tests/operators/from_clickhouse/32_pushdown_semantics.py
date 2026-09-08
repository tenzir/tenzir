# runner: python
"""Pushed predicates select the same rows as their local evaluation.

Every predicate runs twice: in `table` mode, where it is translated into SQL,
and in `sql` mode, where it runs inside Tenzir on the same rows. The outputs
must match. The rows sit on the edges where TQL and ClickHouse could disagree:
NaN and infinities, integers at the 2^53 boundary, a `Float32` value that
`double` widens inexactly, out-of-range set elements, nulls, and quotes.

`system.query_log` confirms that the `table` mode query really carried the
predicate, so a silent fallback to local evaluation cannot make the test pass.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

TABLE = "fc_pushdown_semantics"
CONNECTION = (
    'host=env("CLICKHOUSE_HOST"),\n'
    '  port=int(env("CLICKHOUSE_PORT")),\n'
    '  password=env("CLICKHOUSE_PASSWORD"),\n'
    "  tls=false"
)

# (predicate, fragment that must appear in the pushed WHERE clause, or None
# when the predicate has no exact translation and must stay local)
CASES: list[tuple[str, str | None]] = [
    # Integer column vs literals around 2^53.
    ("i > 0", "`i` > 0"),
    ("i == 4503599627370496", "`i` = 4503599627370496"),
    ("i == 4503599627370496.0", "`i` = 4503599627370496"),
    ("i == 9007199254740992.0", None),
    ("i > 9007199254740992.0", None),
    ("i in [9007199254740993, 0]", "`i` IN (9007199254740993, 0)"),
    # A list with mixed literal types stays local: TQL nulls the clashing
    # elements, ClickHouse would match them.
    ("i in [1, 0.0]", None),
    ("i in [0.0, 1]", None),
    ("i in [0, 18446744073709551615]", None),
    # UInt8 column vs out-of-range and negative literals.
    ("u in [44, 300]", "`u` IN (44, 300)"),
    ("u == 300", "`u` = 300"),
    ("u != 255", "`u` != 255"),
    ("u >= -1", "`u` >= -1"),
    # Float32 column: 0.1 widens inexactly, 16777217 is not representable.
    ("f32 == 0.1", "`f32` = 0.1"),
    ("f32 in [0.1, 0.5]", "`f32` IN (0.1, 0.5)"),
    ("f32 in [0.5, 16777216]", None),
    ("f32 in [16777217]", "`f32` IN (16777217)"),
    ("f32 == 16777217", "`f32` = 16777217"),
    ("f32 >= 16777216", "`f32` >= 16777216"),
    # Float64 column with nan and infinities.
    ("f64 > 0", "`f64` > 0"),
    ("f64 < 0", "`f64` < 0"),
    ("f64 != 1", "`f64` != 1"),
    ("f64 == 9007199254740992", "`f64` = 9007199254740992"),
    ("f64 == 9007199254740993", None),
    ("f64 in [9007199254740992]", "`f64` IN (9007199254740992)"),
    ("not (f64 > 0)", "NOT `f64` > 0"),
    # Nullable column: null-total equality, nan, ordering.
    ("n == null", "`n` IS NULL"),
    ("n != null", "`n` IS NOT NULL"),
    ("not (n == 1.5)", "NOT (`n` IS NOT NULL AND `n` = 1.5)"),
    ("n != 1.5", "(`n` IS NULL OR `n` != 1.5)"),
    ("n > 0", "`n` > 0"),
    ("n in [1.5, -1e300]", "(`n` IS NOT NULL AND `n` IN (1.5, -1e+300))"),
    # Strings with quotes and backslashes.
    ('s == "it\'s"', "`s` = 'it\\'s'"),
    ('s in ["b\\\\c", ""]', "`s` IN ('b\\\\c', '')"),
    ('s != "a"', "`s` != 'a'"),
    # Boolean combinators and a partially translatable conjunction.
    ("i > 0 or f64 < 0", "(`i` > 0 OR `f64` < 0)"),
    ('i > 0 and s.starts_with("a")', "`i` > 0"),
    ('not (i > 0 and s == "a")', "NOT (`i` > 0 AND `s` = 'a')"),
]


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found")


def _clickhouse(sql: str) -> str:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    result = subprocess.run(
        [
            runtime,
            "exec",
            container,
            "clickhouse-client",
            f"--password={password}",
            "--multiquery",
            f"--query={sql}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _run_pipeline(tenzir: tuple[str, ...], pipeline: str) -> str:
    with tempfile.TemporaryDirectory(prefix="fc-semantics-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [*tenzir, "--bare-mode", "--console-verbosity=error", "-f", str(pipe_path)],
            capture_output=True,
            text=True,
            timeout=60,
            env=os.environ.copy(),
        )
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{pipeline}\n{result.stderr}"
    )
    return result.stdout


def _last_select() -> str:
    _clickhouse("SYSTEM FLUSH LOGS")
    output = _clickhouse(
        "SELECT query FROM system.query_log"
        " WHERE type = 'QueryFinish' AND query_kind = 'Select'"
        f" AND has(tables, concat(currentDatabase(), '.{TABLE}'))"
        " ORDER BY event_time_microseconds DESC LIMIT 1"
        " FORMAT TSVRaw"
    )
    assert output, "no SELECT against the test table was logged"
    return output


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    _clickhouse(
        f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
  id UInt64,
  i Int64,
  u UInt8,
  f32 Float32,
  f64 Float64,
  n Nullable(Float64),
  s String
) ENGINE = MergeTree ORDER BY id;
INSERT INTO {TABLE} VALUES
  (1, 9007199254740993, 44, 16777216, 9007199254740992, NULL, 'a'),
  (2, -9007199254740993, 255, 0.1, nan, nan, 'it''s'),
  (3, 0, 0, -0.0, inf, 1.5, 'b\\\\c'),
  (4, 4503599627370496, 1, 0.5, -inf, -1e300, ''),
  (5, 9007199254740992, 2, 16777218, 0, 0, 'a');
"""
    )
    failures: list[str] = []
    for predicate, fragment in CASES:
        pushed = _run_pipeline(
            tenzir,
            f'from_clickhouse table="{TABLE}",\n  {CONNECTION}\n'
            f"where {predicate}\nsort id\nwrite_ndjson",
        )
        query = _last_select()
        if fragment is None:
            if "WHERE" in query:
                failures.append(
                    f"{predicate!r}: expected local evaluation, got {query}"
                )
        elif fragment not in query:
            failures.append(f"{predicate!r}: expected {fragment!r} in {query}")
        local = _run_pipeline(
            tenzir,
            f'from_clickhouse sql="SELECT * FROM {TABLE}",\n  {CONNECTION}\n'
            f"where {predicate}\nsort id\nwrite_ndjson",
        )
        if pushed != local:
            failures.append(
                f"{predicate!r}: pushed and local results differ\n"
                f"--- pushed ({query})\n{pushed}--- local\n{local}"
            )
    assert not failures, "\n".join(failures)
    print(f"ok ({len(CASES)} predicates)")


if __name__ == "__main__":
    main()
