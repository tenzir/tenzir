# runner: python
"""Pushed predicates select the same rows as their local evaluation.

Every predicate runs twice: in `table` mode, where `from_mysql` translates it
into SQL, and in `sql` mode, where it runs inside Tenzir on the same rows. The
outputs must match. The rows sit on the edges where TQL and MySQL could
disagree: integers at the 2^53 and 2^63 boundaries, signed against unsigned,
doubles that need all 17 digits, a `FLOAT` that MySQL sends with six digits,
nulls, and text under collations that ignore case, accents, trailing spaces,
and NUL bytes.

MySQL's general query log confirms what went into SQL, so a silent fallback to
local evaluation cannot make the test pass. Each pipeline carries a second
`where` on `id` with a bound unique to its predicate. The bound never drops a
row, but it is always pushed, so the logged query can be attributed to its
predicate even though the pipelines run concurrently.

A last check covers the other hints: projections, limits, and user SQL.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CONNECTION = (
    'host=env("MYSQL_HOST"),\n'
    '  port=int(env("MYSQL_PORT")),\n'
    '  user=env("MYSQL_USER"),\n'
    '  password=env("MYSQL_PASSWORD"),\n'
    '  database=env("MYSQL_DATABASE")'
)

# (predicate, fragment that must appear in the pushed WHERE clause, or None
# when the predicate must stay local, and the predicate for the local run in
# `sql` mode when the adaptation to the column types changes it)
Case = tuple[str, str | None] | tuple[str, str | None, str]

NUMBER_TABLE = "pd_numbers"
NUMBER_DDL = f"""
DROP TABLE IF EXISTS {NUMBER_TABLE};
CREATE TABLE {NUMBER_TABLE} (
  id INT NOT NULL PRIMARY KEY,
  i BIGINT NOT NULL,
  u TINYINT UNSIGNED NOT NULL,
  ub BIGINT UNSIGNED NOT NULL,
  m MEDIUMINT,
  d DOUBLE NOT NULL,
  n DOUBLE,
  f FLOAT NOT NULL,
  dc DECIMAL(30, 10) NOT NULL,
  dp DOUBLE(10, 2) NOT NULL,
  b BOOL NOT NULL
);
INSERT INTO {NUMBER_TABLE} VALUES
  (1, 9007199254740993, 44, 18446744073709551615, NULL, 9007199254740992,
   NULL, 16777216, 0.1, 1.234, 1),
  (2, -9007199254740993, 255, 0, -8388608, 0.1, 1.5, 0.1, 9007199254740993,
   0, 0),
  (3, 0, 0, 9223372036854775808, 8388607, -0.0, -1e300, 3.14, -1.5, 2.5, 1),
  (4, 4503599627370496, 1, 1, 0, 1e300, 0, 0.5, 0, 1, 0),
  (5, -9223372036854775808, 2, 5, 7, 0.30000000000000004, 0.30000000000000004,
   16777217, 10000000000000000000, 3, 1),
  (6, 9223372036854775807, 3, 9223372036854775807, NULL,
   -2.2250738585072014e-308, NULL, -0.0, 123.456, 4, 0);
"""

NUMBER_CASES: list[Case] = [
    # Signed integers around 2^53 and at both ends of the range.
    ("i > 0", "`i` > 0"),
    ("i == 4503599627370496", "`i` = 4503599627370496"),
    ("i == 4503599627370496.0", "`i` = 4503599627370496e0"),
    ("i == 9007199254740992.0", None),
    ("i in [9007199254740993, 0]", "`i` IN (9007199254740993, 0)"),
    ("i in [1, 0.0]", None),
    ("i == 9223372036854775807", "`i` = 9223372036854775807"),
    ("i < -9223372036854775807", "`i` < -9223372036854775807"),
    ("i < 18446744073709551615", "`i` < 18446744073709551615"),
    ("i > 0.5", "`i` > 0.5e0"),
    # Unsigned integers against negative and large literals, and against
    # signed columns.
    ("ub > -1", "`ub` > -1"),
    ("ub == 18446744073709551615", "`ub` = 18446744073709551615"),
    ("ub >= 9223372036854775808", "`ub` >= 9223372036854775808"),
    # A list with mixed literal types stays local: TQL nulls the clashing
    # elements, MySQL would match them.
    ("ub in [0, 18446744073709551615]", None),
    ("ub in [-1, 5]", "`ub` IN (-1, 5)"),
    ("ub < i", "`ub` < `i`"),
    ("ub == i", "`ub` = `i`"),
    ("u in [44, 300]", "`u` IN (44, 300)"),
    ("u == 300", "`u` = 300"),
    ("u != 255", "`u` != 255"),
    ("u >= -1", "`u` >= -1"),
    # A nullable `MEDIUMINT` at both ends of its range.
    ("m == null", "`m` IS NULL"),
    ("m != null", "`m` IS NOT NULL"),
    ("m != 0", "(`m` IS NULL OR `m` != 0)"),
    ("m > -8388608", "`m` > -8388608"),
    ("not (m == 7)", "NOT (`m` IS NOT NULL AND `m` = 7)"),
    ("m <= 8388607", "`m` <= 8388607"),
    # `BOOL` is a `TINYINT(1)`, which TQL sees as an integer.
    ("b == 1", "`b` = 1"),
    ("b == true", None),
    # Doubles that need all their digits, and negative zero.
    ("d > 0", "`d` > 0e0"),
    ("d < 0", "`d` < 0e0"),
    ("d == 0", "`d` = 0e0"),
    ("d == 0.1", "`d` = 0.1e0"),
    ("d == 0.30000000000000004", "`d` = 0.30000000000000004e0"),
    ("d == 9007199254740992", "`d` = 9007199254740992e0"),
    ("d == 9007199254740993", None),
    ("d in [0.1, 1e300]", "`d` IN (0.1e0, 1e+300)"),
    ("d < 0 and d > -0.001", "`d` < 0e0 AND `d` > -0.001e0"),
    ("d == n", "`d` = `n`"),
    ("d <= n", "`d` <= `n`"),
    ("n == null", "`n` IS NULL"),
    ("n != 1.5", "(`n` IS NULL OR `n` != 1.5e0)"),
    ("n > 0", "`n` > 0e0"),
    ("not (n == 1.5)", "NOT (`n` IS NOT NULL AND `n` = 1.5e0)"),
    # TQL sees a `FLOAT` with six significant digits, `3.14` where MySQL
    # compares `3.1400001`, a `DECIMAL` as the nearest `double`, and a
    # `DOUBLE(10,2)` as padded text.
    ("f == 3.14", None),
    ("f == 16777217", None),
    ("f > 1", None),
    ("dc == 0.1", None),
    ("dc == 9007199254740992", None),
    ("dp == 1.23", None),
    # Boolean combinators and a partially translatable conjunction.
    ("i > 0 or d < 0", "(`i` > 0 OR `d` < 0e0)"),
    ("not (i > 0 and u == 1)", "NOT (`i` > 0 AND `u` = 1)"),
    ("i > 0 and f > 1", "`i` > 0"),
    # Arithmetic stays local.
    ("u + 1 > 2", None),
    ("i / 2 > 1", None),
    ("d * 2.0 > 1", None),
]

STRING_TABLE = "pd_strings"
# `s` and `t` use the default collation, which ignores case, accents, and NUL
# bytes; `g` also ignores trailing spaces. `CHAR` columns lose their padding.
STRING_DDL = f"""
DROP TABLE IF EXISTS {STRING_TABLE};
CREATE TABLE {STRING_TABLE} (
  id INT NOT NULL PRIMARY KEY,
  s VARCHAR(20),
  t TEXT NOT NULL,
  c CHAR(6) NOT NULL,
  g VARCHAR(20) COLLATE utf8mb4_general_ci NOT NULL,
  m VARCHAR(20) CHARACTER SET utf8mb3 NOT NULL,
  a VARCHAR(20) CHARACTER SET ascii NOT NULL,
  l VARCHAR(20) CHARACTER SET latin1 NOT NULL,
  e ENUM('b', 'a') NOT NULL,
  j JSON,
  dt DATETIME NOT NULL,
  vb VARBINARY(8) NOT NULL
) DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
INSERT INTO {STRING_TABLE} VALUES
  (1, 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', '{{"k": 1}}',
   '2024-01-01 00:00:00', 'a'),
  (2, 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'b', '[]', '2024-01-01 12:00:00',
   'A'),
  (3, 'a ', 'a ', 'a ', 'a ', 'a ', 'a ', 'a ', 'a', NULL,
   '1999-12-31 23:59:59', 'a '),
  (4, 'é', 'é', 'é', 'é', 'é', 'e', 'é', 'b', '"x"', '2024-01-01 00:00:00',
   'b'),
  (5, 'e', 'e', 'e', 'e', 'e', 'e', 'e', 'a', '{{}}', '2024-01-01 00:00:00',
   'e'),
  (6, NULL, CONCAT('a', CHAR(0 USING utf8mb4)), 'ab', 'ab', 'ab', 'ab', 'ab',
   'a', NULL, '2024-01-01 00:00:00', X'00'),
  (7, 'it''s', 'b\\\\c', 'x', 'x', 'x', 'x', 'x', 'b', NULL,
   '2024-01-01 00:00:00', 'x'),
  (8, '', '', '', '', '', '', '', 'a', NULL, '2024-01-01 00:00:00', ''),
  (9, 'abcabc', 'ÀB', 'abc', 'abd', 'Σ', 'abc', 'abc', 'a', NULL,
   '2024-01-01 00:00:00', 'abc');
"""


def binary_column(name: str) -> str:
    return f"CAST(`{name}` AS BINARY)"


S = binary_column("s")
T = binary_column("t")
UPPER_A = "_binary'A'"


def folded(text: str) -> str:
    return f"CAST(LOWER(CAST({text} AS CHAR CHARACTER SET utf8mb4)) AS BINARY)"


STRING_CASES: list[Case] = [
    # Equality and order compare bytes, not under the collation.
    ('s == "a"', f"({S} IS NOT NULL AND {S} = _binary'a')"),
    ('s == "A"', f"{S} = _binary'A'"),
    ('s != "a"', f"({S} IS NULL OR {S} != _binary'a')"),
    ('s == "a "', f"{S} = _binary'a '"),
    ('s == "e"', f"{S} = _binary'e'"),
    ('s == "é"', f"{S} = _binary'é'"),
    ('s in ["a", "é"]', f"{S} IN (_binary'a', _binary'é')"),
    ('s < "b"', f"{S} < _binary'b'"),
    ('s >= "a "', f"{S} >= _binary'a '"),
    ('s <= ""', f"{S} <= _binary''"),
    ('s == "it\'s"', f"{S} = _binary'it''s'"),
    ('t == "a"', f"{T} = _binary'a'"),
    ('t == "a\\u0000"', f"{T} = X'6100'"),
    ('t == "b\\\\c"', f"{T} = X'625C63'"),
    ('t in ["b\\\\c", ""]', f"{T} IN (X'625C63', _binary'')"),
    ('t > "b"', f"{T} > _binary'b'"),
    # `CHAR` drops its padding on both sides, so `a ` is stored as `a`.
    ('c == "a"', "CAST(`c` AS BINARY) = _binary'a'"),
    ('c == "a "', "CAST(`c` AS BINARY) = _binary'a '"),
    ('c < "a "', "CAST(`c` AS BINARY) < _binary'a '"),
    # A `PAD SPACE` collation ignores trailing spaces; bytes do not.
    ('g == "a"', "CAST(`g` AS BINARY) = _binary'a'"),
    ('g == "a "', "CAST(`g` AS BINARY) = _binary'a '"),
    ('g in ["A", "e"]', "CAST(`g` AS BINARY) IN (_binary'A', _binary'e')"),
    # Other UTF-8 character sets translate; Latin-1 stays local.
    ('m == "é"', "CAST(`m` AS BINARY) = _binary'é'"),
    ('m == "Σ"', "CAST(`m` AS BINARY) = _binary'Σ'"),
    ('a == "e"', "CAST(`a` AS BINARY) = _binary'e'"),
    ('l == "a"', None),
    ('l == "é"', None),
    # TQL sees the text of enums, JSON, and temporal values; MySQL would
    # compare the values.
    ('e == "a"', None),
    ('e < "b"', None),
    ('j == "[]"', None),
    ('dt == "2024-01-01 00:00:00"', None),
    ('dt > "2024"', None),
    ('vb == "a"', None),
    # Comparisons between two text columns.
    ("s == t", f"{S} = {T}"),
    ("s != t", "NOT ("),
    ("s < t", f"{S} < {T}"),
    ("t == m", f"{T} = CAST(`m` AS BINARY)"),
    ("c == g", "CAST(`c` AS BINARY) = CAST(`g` AS BINARY)"),
    # String functions count bytes.
    ('s.starts_with("a")', f"LEFT({S}, LENGTH(_binary'a')) = _binary'a'"),
    ('s.starts_with("A")', f"LEFT({S}, LENGTH(_binary'A')) = _binary'A'"),
    ('s.starts_with("")', f"LEFT({S}, LENGTH(_binary'')) = _binary''"),
    ('s.ends_with("c")', f"RIGHT({S}, LENGTH(_binary'c')) = _binary'c'"),
    ('s.ends_with(" ")', f"RIGHT({S}, LENGTH(_binary' ')) = _binary' '"),
    ('t.ends_with("\\u0000")', f"RIGHT({T}, LENGTH(X'00')) = X'00'"),
    ('"b" in s', f"INSTR({S}, _binary'b') > 0"),
    ('"" in s', f"INSTR({S}, _binary'') > 0"),
    ('"A" in s', f"INSTR({S}, _binary'A') > 0"),
    ("t.length_bytes() == 2", f"LENGTH({T}) = 2"),
    ("c.length_bytes() in [1, 3]", "LENGTH(CAST(`c` AS BINARY)) IN (1, 3)"),
    (
        's.starts_with("A", ignore_case=true)',
        f"LEFT({folded(S)}, LENGTH({folded(UPPER_A)}))",
    ),
    ('t.starts_with("à", ignore_case=true)', f"LEFT({folded(T)}, LENGTH("),
    ('s.ends_with("ABC", ignore_case=true)', f"RIGHT({folded(S)}, LENGTH("),
    ("s.length_chars() > 2", None),
    # MySQL matches regular expressions with ICU, not RE2.
    ('s.match_regex("^a")', None),
    ('s.to_upper() == "A"', None),
    # A partially translatable conjunction.
    (
        's.starts_with("a") and s.to_upper() == "A "',
        f"LEFT({S}, LENGTH(_binary'a')) = _binary'a'",
    ),
    # An `ip` literal against a text column compares its text; membership in a
    # subnet parses the column, which stays local.
    ("s == 1.2.3.4", f"{S} = _binary'1.2.3.4'", 's == "1.2.3.4"'),
    ("s in 10.0.0.0/8", None, "s.ip() in 10.0.0.0/8"),
]


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found")


def _mysql(sql: str) -> str:
    """Runs `sql` as root and returns the rows, one per line."""
    result = subprocess.run(
        [
            os.environ["MYSQL_CONTAINER_RUNTIME"],
            "exec",
            "-i",
            os.environ["MYSQL_CONTAINER_ID"],
            "mysql",
            "-h",
            "127.0.0.1",
            "-uroot",
            f"-p{os.environ['MYSQL_ROOT_PASSWORD']}",
            "-D",
            os.environ["MYSQL_DATABASE"],
            "--default-character-set=utf8mb4",
            "--batch",
            "--raw",
            "--skip-column-names",
        ],
        input=sql,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"mysql failed: {result.stderr}\n{sql}")
    return result.stdout


def _run_pipeline(tenzir: tuple[str, ...], pipeline: str) -> str:
    with tempfile.TemporaryDirectory(prefix="mysql-semantics-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [*tenzir, "--bare-mode", "--console-verbosity=error", "-f", str(pipe_path)],
            capture_output=True,
            text=True,
            timeout=60,
            env=os.environ.copy(),
        )
    if result.returncode != 0:
        raise RuntimeError(f"pipeline failed:\n{pipeline}\n{result.stderr}")
    return result.stdout


def _logged_selects(table: str) -> list[str]:
    """Returns the `SELECT` queries against `table`, oldest first."""
    output = _mysql(
        "SELECT argument FROM mysql.general_log"
        " WHERE command_type = 'Query' AND user_host NOT LIKE 'root%'"
        " AND argument LIKE 'SELECT %'"
        f" AND (argument LIKE '% FROM `{table}`%'"
        f" OR argument LIKE '% FROM {table}%')"
        " ORDER BY event_time"
    )
    return [line for line in output.splitlines() if line]


def _check(
    tenzir: tuple[str, ...],
    table: str,
    cases: list[Case],
    failures: list[str],
) -> None:
    # A second `where` on `id` tags the query: its bound exceeds every id, so it
    # never drops a row, and it always has an exact translation.
    def tag(index: int) -> str:
        return f"`id` < {1000 + index}"

    def run(item: tuple[int, Case]) -> tuple[str, str]:
        index, case = item
        predicate = case[0]
        local = case[2] if len(case) > 2 else predicate
        tail = f"\nwhere id < {1000 + index}\nsort id\nto_stdout {{ write_ndjson }}"
        try:
            pushed = _run_pipeline(
                tenzir,
                f'from_mysql table="{table}",\n  {CONNECTION}\nwhere {predicate}{tail}',
            )
            local = _run_pipeline(
                tenzir,
                f'from_mysql sql="SELECT * FROM {table}",\n  {CONNECTION}\n'
                f"where {local}{tail}",
            )
        except RuntimeError as exc:
            failures.append(f"{predicate!r}: {exc}")
            return "", ""
        return pushed, local

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run, enumerate(cases)))
    queries = _logged_selects(table)
    # Every query is either tagged or the user's query, as the user wrote it.
    for query in queries:
        if "`id` < 1" not in query and query != f"SELECT * FROM {table}":
            failures.append(f"unexpected query: {query}")
    for index, (case, (pushed, local)) in enumerate(zip(cases, results)):
        predicate, fragment = case[0], case[1]
        pattern = re.compile(re.escape(tag(index)) + r"(?!\d)")
        tagged = [q for q in queries if pattern.search(q)]
        if not tagged:
            failures.append(f"{predicate!r}: no tagged query was logged")
            continue
        query = tagged[-1]
        if fragment is None:
            if query != f"SELECT * FROM `{table}` WHERE {tag(index)}":
                failures.append(
                    f"{predicate!r}: expected local evaluation, got {query}"
                )
        elif fragment not in query:
            failures.append(f"{predicate!r}: expected {fragment!r} in {query}")
        if pushed != local:
            failures.append(
                f"{predicate!r}: pushed and local results differ\n"
                f"--- pushed ({query})\n{pushed}--- local\n{local}"
            )


def _check_hints(tenzir: tuple[str, ...], failures: list[str]) -> None:
    """Checks the query for projections and limits."""
    source = f'from_mysql table="{NUMBER_TABLE}",\n  {CONNECTION}\n'
    cases = [
        # Everything translates: projection, filter, and limit.
        (
            "where i > 0 or m == 7\nselect u, i\nhead 2",
            f"SELECT `i`, `u`, `m` FROM `{NUMBER_TABLE}` WHERE (`i` > 0 OR"
            " (`m` IS NOT NULL AND `m` = 7)) LIMIT 2",
        ),
        # A local predicate keeps its column and takes the limit with it.
        (
            "where i > 0 and f > 1\nselect u\nhead 2",
            f"SELECT `i`, `u`, `f` FROM `{NUMBER_TABLE}` WHERE `i` > 0",
        ),
        # A function call may look at any field.
        (
            "where i > 0 and f.round() == 3\nselect u",
            f"SELECT * FROM `{NUMBER_TABLE}` WHERE `i` > 0",
        ),
        # Without a filter, the limit goes into the query.
        ("head 3\nselect d", f"SELECT `d` FROM `{NUMBER_TABLE}` LIMIT 3"),
        # Unknown fields and names in another case are left to `select`, but
        # the rows must still come.
        ("select missing, ID", f"SELECT `id` FROM `{NUMBER_TABLE}`"),
    ]
    for pipeline, expected in cases:
        _mysql("TRUNCATE TABLE mysql.general_log")
        _run_pipeline(tenzir, f"{source}{pipeline}\nwrite_ndjson")
        queries = _logged_selects(NUMBER_TABLE)
        if queries != [expected]:
            failures.append(f"{pipeline!r}: expected {expected!r}, got {queries}")


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    _mysql(NUMBER_DDL)
    _mysql(STRING_DDL)
    _mysql(
        "SET GLOBAL log_output = 'TABLE';"
        " SET GLOBAL general_log = 'ON';"
        " TRUNCATE TABLE mysql.general_log;"
    )
    failures: list[str] = []
    try:
        _check(tenzir, NUMBER_TABLE, NUMBER_CASES, failures)
        _check(tenzir, STRING_TABLE, STRING_CASES, failures)
        _check_hints(tenzir, failures)
    finally:
        _mysql(
            "SET GLOBAL general_log = 'OFF';"
            " TRUNCATE TABLE mysql.general_log;"
            f" DROP TABLE IF EXISTS {NUMBER_TABLE}, {STRING_TABLE};"
        )
    assert not failures, "\n".join(failures)
    total = len(NUMBER_CASES) + len(STRING_CASES)
    print(f"ok ({total} predicates)")


if __name__ == "__main__":
    main()
