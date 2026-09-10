# runner: python
"""Assert the SQL that `from_clickhouse` sends when the optimizer pushes hints.

The tests around this one check the pipeline output, which is identical whether
a predicate runs in ClickHouse or locally. This test reads back the queries from
`system.query_log` to prove what went into SQL and what stayed behind.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

TABLE = "fc_pushdown_log"
CONNECTION = (
    'host=env("CLICKHOUSE_HOST"),\n'
    '  port=int(env("CLICKHOUSE_PORT")),\n'
    '  password=env("CLICKHOUSE_PASSWORD"),\n'
    "  tls=false"
)


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
    with tempfile.TemporaryDirectory(prefix="fc-pushdown-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=warning",
                "-f",
                str(pipe_path),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=os.environ.copy(),
        )
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{result.stdout}\n{result.stderr}"
    )
    return result.stdout


def _logged_queries(kind: str) -> list[str]:
    """Returns the queries of `kind` against the test table, oldest first."""
    _clickhouse("SYSTEM FLUSH LOGS")
    output = _clickhouse(
        "SELECT query FROM system.query_log"
        f" WHERE type = 'QueryFinish' AND query_kind = '{kind}'"
        f" AND has(tables, concat(currentDatabase(), '.{TABLE}'))"
        " ORDER BY event_time_microseconds"
        " FORMAT TSVRaw"
    )
    return [line for line in output.splitlines() if line]


def _last_select() -> str:
    selects = _logged_queries("Select")
    assert selects, "no SELECT against the test table was logged"
    return selects[-1]


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    _clickhouse(
        f"""
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE {TABLE} (
  id UInt64,
  x Int64,
  y String,
  n Nullable(Int64),
  meta Tuple(source String, level Int64),
  half Tuple(ok String, bad Map(String, Int64)),
  d Int64 DEFAULT x * 10,
  a String ALIAS concat(y, '-alias'),
  m Int64 MATERIALIZED x + 1
) ENGINE = MergeTree ORDER BY id;
INSERT INTO {TABLE} (id, x, y, n, meta, half) VALUES (1, 1, 'foo', NULL, ('a', 1), ('x', map('k', 1))), (2, -1, 'bar', 2, ('b', 2), ('y', map())), (3, 0, '1.1.1.1', NULL, ('c', 3), ('z', map()));
"""
    )
    source = f'from_clickhouse table="{TABLE}",\n  {CONNECTION}\n'
    # Everything translates: filter, projection, and limit go into the query.
    # The projection covers the filter's references, in table order.
    _run_pipeline(
        tenzir,
        source + 'where x > 0 or y == "foo" and n in [1, 2, 3]\nselect x, y\nhead 42',
    )
    assert _last_select() == (
        f"SELECT `x`, `y`, `n` FROM {TABLE}"
        " WHERE (`x` > 0 OR (`y` = 'foo' AND (`n` IS NOT NULL AND `n` IN (1, 2, 3))))"
        " LIMIT 42"
    ), _last_select()
    # A conjunct without translation stays local and takes the limit with it.
    _run_pipeline(tenzir, source + 'where x > 0 and y.to_upper() == "FOO"\nhead 5')
    assert _last_select() == f"SELECT * FROM {TABLE} WHERE `x` > 0", _last_select()
    # String functions with a ClickHouse counterpart go into the query.
    _run_pipeline(
        tenzir,
        source + 'where y.starts_with("f") and "a" in y and y.length_bytes() > 2',
    )
    assert _last_select() == (
        f"SELECT * FROM {TABLE} WHERE startsWith(`y`, 'f') AND position(`y`, 'a') > 0"
        " AND length(`y`) > 2"
    ), _last_select()
    # Two columns compare directly; arithmetic and literal folding translate.
    _run_pipeline(tenzir, source + "where x < meta.level and x / 2 > 1 - 2")
    assert _last_select() == (
        f"SELECT * FROM {TABLE} WHERE `x` < `meta`.`level` AND (`x` / 2) > -1"
    ), _last_select()
    # A nested projection narrows the tuple to the requested elements.
    output = _run_pipeline(tenzir, source + "select id, meta.level\nwrite_ndjson")
    assert _last_select() == (
        f"SELECT `id`, CAST(tuple(`meta`.`level`), 'Tuple(`level` Int64)') AS `meta`"
        f" FROM {TABLE}"
    ), _last_select()
    assert output.strip().splitlines() == [
        '{"id":1,"meta":{"level":1}}',
        '{"id":2,"meta":{"level":2}}',
        '{"id":3,"meta":{"level":3}}',
    ], output
    # A tuple with an element the operator cannot decode is dropped locally as
    # a whole. Narrowing it to a decodable element would expose a value where
    # the unoptimized pipeline yields `null`, so it is selected whole and the
    # local `select` sees no such field.
    output = _run_pipeline(tenzir, source + "select id, half.ok\nwrite_ndjson")
    assert _last_select() == f"SELECT `id`, `half` FROM {TABLE}", _last_select()
    assert output.strip().splitlines() == [
        '{"id":1,"half":{"ok":null}}',
        '{"id":2,"half":{"ok":null}}',
        '{"id":3,"half":{"ok":null}}',
    ], output
    # Nullable equality carries a null guard so `not` keeps TQL semantics.
    _run_pipeline(tenzir, source + "where not (n == 2)")
    assert _last_select() == (
        f"SELECT * FROM {TABLE} WHERE NOT (`n` IS NOT NULL AND `n` = 2)"
    ), _last_select()
    # Tuple elements become compound identifiers.
    _run_pipeline(tenzir, source + "where meta.level > 1")
    assert _last_select() == (f"SELECT * FROM {TABLE} WHERE `meta`.`level` > 1"), (
        _last_select()
    )
    # An `ip` literal against a `String` column compares against its text, and
    # a subnet parses the column locally behind a prefilter, which keeps the
    # limit local as well.
    output = _run_pipeline(
        tenzir, source + "where y == 1.1.1.1\nselect id\nwrite_ndjson"
    )
    assert _last_select() == (f"SELECT `id`, `y` FROM {TABLE} WHERE `y` = '1.1.1.1'"), (
        _last_select()
    )
    assert output.strip() == '{"id":3}', output
    output = _run_pipeline(
        tenzir, source + "where y in 1.0.0.0/8\nhead 1\nselect id\nwrite_ndjson"
    )
    assert _last_select() == (
        f"SELECT `id`, `y` FROM {TABLE} WHERE (toIPv6OrNull(`y`) IS NULL OR"
        " toIPv6OrNull(`y`) BETWEEN toIPv6('1.0.0.0') AND toIPv6('1.255.255.255'))"
    ), _last_select()
    assert output.strip() == '{"id":3}', output
    # A limit alone needs no schema round-trip.
    describes_before = len(_logged_queries("Describe"))
    _run_pipeline(tenzir, source + "head 1")
    assert _last_select() == f"SELECT * FROM {TABLE} LIMIT 1", _last_select()
    assert len(_logged_queries("Describe")) == describes_before, (
        "unexpected DESCRIBE for a limit-only pipeline"
    )
    # `SELECT *` omits ALIAS and MATERIALIZED columns, so a predicate on one
    # stays local, where it sees no such field and drops every row. Pushing it
    # would match rows instead. A DEFAULT column is an ordinary column.
    output = _run_pipeline(
        tenzir, source + 'where a == "foo-alias" or m == 2\nwrite_ndjson'
    )
    assert _last_select() == f"SELECT * FROM {TABLE}", _last_select()
    assert output.strip() == "", output
    output = _run_pipeline(tenzir, source + "where d == 10\nselect id\nwrite_ndjson")
    assert _last_select() == (f"SELECT `id`, `d` FROM {TABLE} WHERE `d` = 10"), (
        _last_select()
    )
    assert output.strip() == '{"id":1}', output
    # A projection that names a generated column keeps `SELECT *`, because
    # whether that includes the column depends on the session's settings. With
    # the defaults it does not, and `select` fills the field with null.
    output = _run_pipeline(tenzir, source + "select id, a, m\nhead 1\nwrite_ndjson")
    assert _last_select() == f"SELECT * FROM {TABLE} LIMIT 1", _last_select()
    assert output.strip() == '{"id":1,"a":null,"m":null}', output
    # A user whose profile includes generated columns in `SELECT *` gets their
    # computed values, as before pushdown.
    _clickhouse(
        """
DROP USER IF EXISTS fc_asterisk;
CREATE USER fc_asterisk IDENTIFIED BY 'fc_asterisk'
  SETTINGS asterisk_include_alias_columns = 1,
           asterisk_include_materialized_columns = 1;
GRANT SELECT ON default.* TO fc_asterisk;
"""
    )
    as_asterisk = (
        f'from_clickhouse table="{TABLE}",\n'
        '  host=env("CLICKHOUSE_HOST"),\n'
        '  port=int(env("CLICKHOUSE_PORT")),\n'
        '  user="fc_asterisk",\n'
        '  password="fc_asterisk",\n'
        "  tls=false\n"
    )
    output = _run_pipeline(
        tenzir, as_asterisk + "select id, a, m\nhead 1\nwrite_ndjson"
    )
    assert _last_select() == f"SELECT * FROM {TABLE} LIMIT 1", _last_select()
    assert output.strip() == '{"id":1,"a":"foo-alias","m":2}', output
    output = _run_pipeline(
        tenzir, as_asterisk + 'where a == "foo-alias"\nselect id\nwrite_ndjson'
    )
    assert _last_select() == f"SELECT * FROM {TABLE}", _last_select()
    assert output.strip() == '{"id":1}', output
    # User-provided SQL is sent verbatim.
    _run_pipeline(
        tenzir,
        f'from_clickhouse sql="SELECT * FROM {TABLE}",\n  {CONNECTION}\nwhere x > 0\nhead 1',
    )
    assert _last_select() == f"SELECT * FROM {TABLE}", _last_select()
    print("ok")


if __name__ == "__main__":
    main()
