# runner: python
"""Pushed predicates select the same rows as their local evaluation.

Every predicate runs twice: in `table` mode, where `from_microsoft_sql`
translates it into T-SQL, and in `sql` mode, where it runs inside Tenzir on the
same rows. The outputs must match. The rows sit on the edges where TQL and SQL
Server could disagree: integers at the 2^53 and 2^63 boundaries, a `real` that
`double` widens inexactly, `bit` columns, nulls, text that differs only in
case, accents, trailing spaces, tabs, or NUL bytes, supplementary characters,
text in a code page, and timestamps at both ends of what TQL can represent and
in other time zones.

SQL Server's plan cache confirms what went into the query, so a silent fallback
to local evaluation cannot make the test pass. Each pipeline carries a second
`where` on `id` with a bound unique to its predicate. The bound never drops a
row, but it is always pushed, so the cached query can be attributed to its
predicate even though the pipelines run concurrently.

A last check covers the other hints: projections and limits.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def _connection(user: str, password: str) -> str:
    return (
        'host=env("MSSQL_HOST"),\n'
        '  port=int(env("MSSQL_PORT")),\n'
        f'  user=env("{user}"),\n'
        f'  password=env("{password}"),\n'
        '  database=env("MSSQL_DATABASE")'
    )


CONNECTION = _connection("MSSQL_USER", "MSSQL_PASSWORD")
ADMIN = _connection("MSSQL_ADMIN_USER", "MSSQL_ADMIN_PASSWORD")

# (predicate, fragment that must appear in the pushed WHERE clause, or None
# when the predicate must stay local, and the predicate for the local run in
# `sql` mode when the adaptation to the column types changes it)
Case = tuple[str, str | None] | tuple[str, str | None, str]

NUMBER_TABLE = "pd_numbers"
NUMBER_DDL = f"""
DROP TABLE IF EXISTS {NUMBER_TABLE};
CREATE TABLE {NUMBER_TABLE} (
  id INT NOT NULL PRIMARY KEY,
  n INT NULL,
  tiny TINYINT NOT NULL,
  big BIGINT NOT NULL,
  b BIT NOT NULL,
  nb BIT NULL,
  f FLOAT NOT NULL,
  r REAL NOT NULL,
  dc DECIMAL(30, 10) NOT NULL,
  m MONEY NOT NULL
);
INSERT INTO {NUMBER_TABLE} VALUES
  (1, NULL, 44, 9007199254740993, 1, NULL, 9007199254740992, 16777216, 0.1,
   1.5),
  (2, -2147483648, 255, -9007199254740993, 0, 1, 0.1, 0.1, 9007199254740993,
   0),
  (3, 2147483647, 0, -9223372036854775808, 1, 0, -0.0, 3.14, -1.5, 2.5),
  (4, 0, 1, 4503599627370496, 0, NULL, 1e300, 0.5, 0, 1),
  (5, 7, 2, 9223372036854775807, 1, 1, 0.30000000000000004, 16777217, 123.456,
   3),
  (6, NULL, 3, 0, 0, 0, -1e-300, 1.5, 1, 4);
"""

NUMBER_CASES: list[Case] = [
    # Integers around 2^53 and at both ends of the range.
    ("big > 0", "[big] > 0"),
    ("big == 4503599627370496", "[big] = 4503599627370496"),
    ("big == 4503599627370496.0", "[big] = 4503599627370496e0"),
    ("big == 9007199254740992.0", None),
    ("big in [9007199254740993, 0]", "[big] IN (9007199254740993, 0)"),
    ("big == 9223372036854775807", "[big] = 9223372036854775807"),
    ("big == -9223372036854775808", "[big] = -9223372036854775808"),
    ("big < 18446744073709551615", "[big] < 18446744073709551615"),
    ("big > 0.5", "[big] > 0.5e0"),
    # A `tinyint` against out-of-range and negative literals.
    ("tiny in [44, 300]", "[tiny] IN (44, 300)"),
    ("tiny == 300", "[tiny] = 300"),
    ("tiny >= -1", "[tiny] >= -1"),
    ("tiny < big", "[tiny] < [big]"),
    # A nullable `int` at both ends of its range.
    ("n == null", "[n] IS NULL"),
    ("n != null", "[n] IS NOT NULL"),
    ("n != 0", "([n] IS NULL OR [n] != 0)"),
    ("not (n == 7)", "NOT ([n] IS NOT NULL AND [n] = 7)"),
    ("n >= -2147483648", "[n] >= -2147483648"),
    # `bit` columns are predicates and operands.
    ("b", "[b] = 1"),
    ("not b", "NOT [b] = 1"),
    ("b == false", "[b] = 0"),
    ("b in [false]", "[b] IN (0)"),
    ("nb == true", "([nb] IS NOT NULL AND [nb] = 1)"),
    ("nb != true", "([nb] IS NULL OR [nb] != 1)"),
    ("nb == null", "[nb] IS NULL"),
    ("b == nb", "([nb] IS NOT NULL AND [b] = [nb])"),
    ("b or big > 0", "([b] = 1 OR [big] > 0)"),
    # Floats need all their digits; `real` widens to `double` exactly.
    ("f > 0", "[f] > 0e0"),
    ("f == 0", "[f] = 0e0"),
    ("f == 0.1", "[f] = 0.1e0"),
    ("f == 0.30000000000000004", "[f] = 0.30000000000000004e0"),
    ("f == 9007199254740993", None),
    ("f in [0.1, 1e300]", "[f] IN (0.1e0, 1e+300)"),
    ("f < 0", "[f] < 0e0"),
    ("r == 0.1", "[r] = 0.1e0"),
    ("r == 16777216", "[r] = 16777216e0"),
    ("r == 16777217", "[r] = 16777217e0"),
    ("r > 3.13", "[r] > 3.13e0"),
    ("f < r", "[f] < [r]"),
    # TQL sees a `decimal` and `money` as the nearest `double`.
    ("dc == 0.1", None),
    ("dc == 9007199254740992", None),
    ("m > 1", None),
    # Arithmetic widens to the result type.
    ("tiny + 1 > 255", "(CAST([tiny] AS bigint) + 1) > 255"),
    ("n + 1 > 2147483647", "(CAST([n] AS bigint) + 1) > 2147483647"),
    ("n - 1 < -2147483648", "(CAST([n] AS bigint) - 1) < -2147483648"),
    ("n / 2 > 3", "(CAST([n] AS float) / 2) > 3e0"),
    ("r * 2 > 1", "(CAST([r] AS float) * 2) > 1e0"),
    ("big + 1 > 0", None),
    # Boolean combinators and a partially translatable conjunction.
    ("not (big > 0 and tiny == 1)", "NOT ([big] > 0 AND [tiny] = 1)"),
    ("big > 0 and dc > 1", "[big] > 0"),
]

STRING_TABLE = "pd_strings"
# `s`, `t`, and `v` use the server's case-insensitive default collation; `vu`
# stores UTF-8, and `v` stores code page 1252, whose bytes TQL sees as they are.
STRING_DDL = f"""
DROP TABLE IF EXISTS {STRING_TABLE};
CREATE TABLE {STRING_TABLE} (
  id INT NOT NULL PRIMARY KEY,
  s NVARCHAR(20) NULL,
  t NCHAR(4) NOT NULL,
  v VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  vu VARCHAR(20) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL,
  u UNIQUEIDENTIFIER NULL,
  x NVARCHAR(MAX) NULL
);
INSERT INTO {STRING_TABLE} VALUES
  (1, N'a', N'a', 'a', 'a', '61F0C404-5CB3-11E7-907B-A6006AD3DBA0', N'a'),
  (2, N'A', N'A', 'A', 'A', '00000000-0000-0000-0000-000000000000', N'A'),
  (3, N'a ', N'a ', 'a ', 'a ', NULL, N'a '),
  (4, N'é', N'é', 'e', N'é', '61F0C404-5CB3-11E7-907B-A6006AD3DBA1', N'é'),
  (5, N'e', N'e', 'e', 'e', '61F0C404-5CB3-11E7-907B-A6006AD3DBA2', N'e'),
  (6, N'a' + NCHAR(9), N'a' + NCHAR(9), 'a' + CHAR(9), 'a' + CHAR(9),
   'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF', N'a' + NCHAR(9)),
  (7, N'a' + NCHAR(0), N'a' + NCHAR(0), 'a' + CHAR(0), 'a' + CHAR(0), NULL,
   N'a' + NCHAR(0)),
  (8, N'😀', N'Σ', 'b', N'😀', NULL, N'😀'),
  (9, N'', N'', '', '', NULL, N''),
  (10, NULL, N'ab', 'ab', 'ab', NULL, NULL),
  (11, N'it''s', N'b\\c', 'it''s', 'b\\c', NULL, N'it''s'),
  (12, N'abcabc', N'ÀB', 'abc', N'ÀB', NULL, REPLICATE(CAST(N'ab' AS NVARCHAR(MAX)), 5000) + N'é'),
  (13, NCHAR(65533), NCHAR(65533), 'é', N'Ω', NULL, NCHAR(65533));
"""


def binary(name: str) -> str:
    return f"CAST(CAST([{name}] COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(max)) AS varbinary(max))"


def hexed(bytes_sql: str) -> str:
    return f"CONVERT(varchar(max), {bytes_sql}, 2) COLLATE Latin1_General_BIN2"


S = hexed(binary("s"))
T = hexed(binary("t"))
V = hexed("CAST([v] AS varbinary(max))")
VU = hexed("CAST([vu] AS varbinary(max))")
U = hexed("CAST(LOWER(CAST([u] AS char(36))) AS varbinary(max))")
X = hexed(binary("x"))


def h(text: str) -> str:
    return text.encode().hex().upper()


UUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
IT_S = "it's"

STRING_CASES: list[Case] = [
    # Equality and order compare bytes, not under the collation, and without
    # padding.
    ('s == "a"', f"({S} IS NOT NULL AND {S} = '61')"),
    ('s == "A"', f"{S} = '41'"),
    ('s != "a"', f"({S} IS NULL OR {S} != '61')"),
    ('s == "a "', f"{S} = '6120'"),
    ('s == "e"', f"{S} = '65'"),
    ('s == "é"', f"{S} = 'C3A9'"),
    ('s == "😀"', f"{S} = '{h('😀')}'"),
    ('s == "\ufffd"', f"{S} = 'EFBFBD'"),
    ('s in ["a", "é"]', f"{S} IN ('61', 'C3A9')"),
    ('s < "b"', f"{S} < '62'"),
    ('s > "a"', f"{S} > '61'"),
    ('s >= "a "', f"{S} >= '6120'"),
    ('s < "a "', f"{S} < '6120'"),
    ('s <= ""', f"{S} <= ''"),
    ('s > "\ufffd"', f"{S} > 'EFBFBD'"),
    ('s == "it\'s"', f"{S} = '{h(IT_S)}'"),
    ('s == "a\\t"', f"{S} = '6109'"),
    ('s == "a\\u0000"', f"{S} = '6100'"),
    ("s == x", f"{S} = {X}"),
    ("s < x", f"{S} < {X}"),
    # `nchar` keeps its padding in TQL.
    ('t == "a"', f"{T} = '61'"),
    ('t == "a   "', f"{T} = '61202020'"),
    ('t == "b\\\\c "', f"{T} = '625C6320'"),
    ('t < "a "', f"{T} < '6120'"),
    # Text in a code page compares the bytes that TQL sees.
    ('v == "a"', f"{V} = '61'"),
    ('v == "A"', f"{V} = '41'"),
    ('v == "a "', f"{V} = '6120'"),
    ('v in ["e", "b"]', f"{V} IN ('65', '62')"),
    ('v < "b"', f"{V} < '62'"),
    # TQL sees the code page byte of `é`, which no UTF-8 literal equals.
    ('v == "é"', f"{V} = 'C3A9'"),
    ('v > "z"', f"{V} > '7A'"),
    ('vu == "é"', f"{VU} = 'C3A9'"),
    ('vu == "ÀB"', f"{VU} = '{h('ÀB')}'"),
    ("v == vu", f"{V} = {VU}"),
    ("t == vu", f"{T} = {VU}"),
    # UUIDs compare as their lowercase text.
    (f'u == "{UUID}"', f"{U} = '{h(UUID)}'"),
    (f'u == "{UUID.upper()}"', f"{U} = '{h(UUID.upper())}'"),
    ('u < "1"', f"{U} < '31'"),
    ('u.starts_with("61f0")', f"LEFT({U}, LEN('{h('61f0')}')) = '{h('61f0')}'"),
    # String functions count bytes.
    ('s.starts_with("a")', f"LEFT({S}, LEN('61')) = '61'"),
    ('s.starts_with("A")', f"LEFT({S}, LEN('41')) = '41'"),
    ('s.starts_with("")', f"LEFT({S}, LEN('')) = ''"),
    ('s.starts_with("ab")', f"LEFT({S}, LEN('6162')) = '6162'"),
    ('s.ends_with("c")', f"RIGHT({S}, LEN('63')) = '63'"),
    ('s.ends_with(" ")', f"RIGHT({S}, LEN('20')) = '20'"),
    ('s.ends_with("\\t")', f"RIGHT({S}, LEN('09')) = '09'"),
    ('t.ends_with("\\u0000   ")', f"RIGHT({T}, LEN('00202020')) = '00202020'"),
    ('x.ends_with("bé")', f"RIGHT({X}, LEN('62C3A9')) = '62C3A9'"),
    ('"b" in s', f"CHARINDEX(0x62, {binary('s')}) > 0"),
    ('"" in s', f"DATALENGTH({binary('s')}) >= 0"),
    ('"é" in x', f"CHARINDEX(0xC3A9, {binary('x')}) > 0"),
    ('"\\u0000" in v', "CHARINDEX(0x00, CAST([v] AS varbinary(max))) > 0"),
    ('"A" in s', f"CHARINDEX(0x41, {binary('s')}) > 0"),
    ("s.length_bytes() > 2", f"(LEN({S}) / 2) > 2"),
    ("t.length_bytes() == 8", f"(LEN({T}) / 2) = 8"),
    ("x.length_bytes() > 10000", f"(LEN({X}) / 2) > 10000"),
    ('s.starts_with("A", ignore_case=true)', None),
    ('s.match_regex("^a")', None),
    ("s.length_chars() > 2", None),
    # A partially translatable conjunction.
    ('s.starts_with("a") and s.to_upper() == "A "', f"LEFT({S}, LEN('61')) = '61'"),
    # An `ip` literal against a text column compares its text; membership in a
    # subnet parses the column, which stays local.
    ("s == 1.2.3.4", f"{S} = '{h('1.2.3.4')}'", 's == "1.2.3.4"'),
    ("s in 10.0.0.0/8", None, "s.ip() in 10.0.0.0/8"),
]

TIME_TABLE = "pd_times"
# Row 2 holds the first day and the last instant that TQL can represent.
TIME_DDL = f"""
DROP TABLE IF EXISTS {TIME_TABLE};
CREATE TABLE {TIME_TABLE} (
  id INT NOT NULL PRIMARY KEY,
  d DATE NOT NULL,
  dn DATE NULL,
  ts DATETIME2(3) NOT NULL,
  ts7 DATETIME2(7) NOT NULL,
  ts0 DATETIME2(0) NOT NULL,
  tz DATETIMEOFFSET(7) NOT NULL,
  sd SMALLDATETIME NOT NULL,
  dt DATETIME NOT NULL,
  tm TIME NOT NULL
);
INSERT INTO {TIME_TABLE} VALUES
  (1, '2024-01-01', NULL, '2024-01-01T00:00:00.123', '2024-01-01T00:00:00.1234567',
   '2024-01-01T00:00:01', '2024-01-01T01:00:00+01:00', '2024-01-01T12:34:00',
   '2024-01-01T00:00:00.003', '12:00:00'),
  (2, '1677-09-22', '2262-04-11', '2262-04-11T23:47:16.854',
   '2262-04-11T23:47:16.8547750', '1677-09-22T00:00:00',
   '2262-04-11T23:47:16.8547750+00:00', '2079-06-06T23:59:00',
   '1753-01-01', '00:00:00'),
  (3, '1970-01-01', '1970-01-01', '1970-01-01T00:00:00', '1969-12-31T23:59:59.9999999',
   '1970-01-01T00:00:00', '1969-12-31T19:00:00-05:00', '1900-01-01T00:00:00',
   '1970-01-01', '23:59:59.9999999'),
  (4, '2024-02-29', '2024-02-29', '2024-02-29T23:59:59.999', '2024-02-29T23:59:59.9999999',
   '2024-02-29T23:59:59', '2024-03-01T01:59:59.9999999+02:00', '2024-02-29T23:59:00',
   '2024-02-29T23:59:59.997', '01:02:03'),
  (5, '2024-01-02', NULL, '2024-01-01T23:59:59.999', '2024-01-02T00:00:00',
   '2024-01-02T00:00:00', '2024-01-01T23:00:00-01:00', '2024-01-01T12:35:00',
   '2024-01-02', '00:00:01');
"""

TIME_CASES: list[Case] = [
    # `date` against instants on a day, between days, and at the ends.
    ("d == 2024-01-01", "[d] = CAST('2024-01-01' AS date)"),
    ("d == 2024-01-01T12:00:00", "WHERE 0 = 1"),
    ("d != 2024-01-01T12:00:00", "WHERE 1 = 1"),
    ("d < 2024-01-01T12:00:00", "[d] <= CAST('2024-01-01' AS date)"),
    ("d >= 2024-01-01T12:00:00", "[d] > CAST('2024-01-01' AS date)"),
    ("d < 1900-01-01", "[d] < CAST('1900-01-01' AS date)"),
    ("dn == 2262-04-11", "([dn] IS NOT NULL AND [dn] = CAST('2262-04-11' AS date))"),
    (
        "d in [2024-01-01, 2024-01-01T00:00:01, 1970-01-01]",
        "[d] IN (CAST('2024-01-01' AS date), CAST('1970-01-01' AS date))",
    ),
    ("dn == null", "[dn] IS NULL"),
    ("dn != 2024-02-29", "([dn] IS NULL OR [dn] != CAST('2024-02-29' AS date))"),
    ("d == dn", "([dn] IS NOT NULL AND [d] = [dn])"),
    # `datetime2` at every precision, with literals between ticks.
    (
        "ts > 2024-01-01T00:00:00.1234",
        "[ts] > CAST('2024-01-01T00:00:00.123' AS datetime2(3))",
    ),
    ("ts == 2024-01-01T00:00:00.1234", "WHERE 0 = 1"),
    (
        "ts == 2024-01-01T00:00:00.123",
        "[ts] = CAST('2024-01-01T00:00:00.123' AS datetime2(3))",
    ),
    (
        "ts >= 2262-04-11T23:47:16.854",
        "CAST('2262-04-11T23:47:16.854' AS datetime2(3))",
    ),
    (
        "ts7 == 2024-01-01T00:00:00.1234567",
        "[ts7] = CAST('2024-01-01T00:00:00.1234567' AS datetime2(7))",
    ),
    ("ts7 < 1970-01-01", "[ts7] < CAST('1970-01-01T00:00:00.0000000' AS datetime2(7))"),
    (
        "ts7 <= 2262-04-11T23:47:16.854775807",
        "CAST('2262-04-11T23:47:16.8547758' AS datetime2(7))",
    ),
    (
        "ts0 == 2024-01-01T00:00:01",
        "[ts0] = CAST('2024-01-01T00:00:01' AS datetime2(0))",
    ),
    ("ts0 < 1900-01-01", "[ts0] < CAST('1900-01-01T00:00:00' AS datetime2(0))"),
    ("ts == ts7", None),
    ("ts < ts", "[ts] < [ts]"),
    # `datetimeoffset` compares instants, whatever the offset.
    (
        "tz == 2024-01-01T00:00:00",
        "[tz] = CAST('2024-01-01T00:00:00.0000000+00:00' AS datetimeoffset(7))",
    ),
    (
        "tz < 1970-01-01",
        "[tz] < CAST('1970-01-01T00:00:00.0000000+00:00' AS datetimeoffset(7))",
    ),
    ("tz > 2024-02-29T23:59:59.9999998", "AS datetimeoffset(7))"),
    # `smalldatetime` holds whole minutes from 1900 to 2079.
    (
        "sd == 2024-01-01T12:34:00",
        "[sd] = CAST('2024-01-01T12:34:00' AS smalldatetime)",
    ),
    ("sd == 2024-01-01T12:34:30", "WHERE 0 = 1"),
    (
        "sd < 2024-01-01T12:34:30",
        "[sd] <= CAST('2024-01-01T12:34:00' AS smalldatetime)",
    ),
    ("sd > 2100-01-01", "smalldatetime"),
    ("sd <= 1900-01-01", "[sd] <= CAST('1900-01-01T00:00:00' AS smalldatetime)"),
    # `datetime` and `time` stay local.
    ("dt > 2024-01-01", None),
    ("dt == 2024-01-01T00:00:00.003", None),
    ("tm > 1h", None),
]


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found")


def _run_pipeline(tenzir: tuple[str, ...], pipeline: str) -> str:
    with tempfile.TemporaryDirectory(prefix="mssql-semantics-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [*tenzir, "--bare-mode", "--console-verbosity=error", "-f", str(pipe_path)],
            capture_output=True,
            text=True,
            timeout=120,
            env=os.environ.copy(),
        )
    if result.returncode != 0:
        raise RuntimeError(f"pipeline failed:\n{pipeline}\n{result.stderr}")
    return result.stdout


def _admin_sql(tenzir: tuple[str, ...], sql: str, output: str = "discard") -> str:
    escaped = sql.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    return _run_pipeline(
        tenzir, f'from_microsoft_sql sql="{escaped}",\n  {ADMIN}\n{output}'
    )


def _cached_selects(tenzir: tuple[str, ...], table: str) -> list[str]:
    """Returns the ad hoc `SELECT` queries against `table` in the plan cache."""
    output = _admin_sql(
        tenzir,
        "SELECT CAST(t.text AS nvarchar(max)) AS q "
        "FROM sys.dm_exec_cached_plans AS p "
        "CROSS APPLY sys.dm_exec_sql_text(p.plan_handle) AS t "
        f"WHERE p.objtype = 'Adhoc' AND t.text LIKE N'SELECT % FROM [[]{table}]%'",
        "write_ndjson",
    )
    return [json.loads(line)["q"] for line in output.splitlines() if line]


def _check(
    tenzir: tuple[str, ...],
    table: str,
    cases: list[Case],
    failures: list[str],
) -> None:
    # A second `where` on `id` tags the query: its bound exceeds every id, so it
    # never drops a row, and it always has an exact translation.
    def tag(index: int) -> str:
        return f"[id] < {1000 + index}"

    def run(item: tuple[int, Case]) -> tuple[str, str]:
        index, case = item
        predicate = case[0]
        local = case[2] if len(case) > 2 else predicate
        tail = f"\nwhere id < {1000 + index}\nsort id\nselect id\nto_stdout {{ write_ndjson }}"
        try:
            pushed = _run_pipeline(
                tenzir,
                f'from_microsoft_sql table="{table}",\n  {CONNECTION}\n'
                f"where {predicate}{tail}",
            )
            local = _run_pipeline(
                tenzir,
                f'from_microsoft_sql sql="SELECT * FROM {table}",\n  {CONNECTION}\n'
                f"where {local}{tail}",
            )
        except RuntimeError as exc:
            failures.append(f"{predicate!r}: {exc}")
            return "", ""
        return pushed, local

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run, enumerate(cases)))
    queries = _cached_selects(tenzir, table)
    for index, (case, (pushed, local)) in enumerate(zip(cases, results)):
        predicate, fragment = case[0], case[1]
        pattern = re.compile(re.escape(tag(index)) + r"(?!\d)")
        tagged = [q for q in queries if pattern.search(q)]
        if not tagged:
            failures.append(f"{predicate!r}: no tagged query was cached")
            continue
        query = tagged[-1]
        if fragment is None:
            if not query.endswith(f"FROM [{table}] WHERE {tag(index)}"):
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
    source = f'from_microsoft_sql table="{NUMBER_TABLE}",\n  {CONNECTION}\n'
    cases = [
        # Everything translates: projection, filter, and limit.
        (
            "where big > 0 or n == 7\nselect tiny, big\nhead 2",
            f"SELECT TOP (2) [n], [tiny], [big] FROM [{NUMBER_TABLE}] WHERE ([big] > 0 OR"
            " ([n] IS NOT NULL AND [n] = 7))",
        ),
        # A local predicate keeps its column and takes the limit with it.
        (
            "where big > 1 and dc > 1\nselect tiny\nhead 2",
            f"SELECT [tiny], [big], [dc] FROM [{NUMBER_TABLE}] WHERE [big] > 1",
        ),
        # Without a filter, the limit goes into the query.
        ("head 3\nselect f", f"SELECT TOP (3) [f] FROM [{NUMBER_TABLE}]"),
        # Unknown fields and names in another case are left to `select`, but
        # the rows must still come.
        ("select missing, ID", f"SELECT [id] FROM [{NUMBER_TABLE}]"),
    ]
    for pipeline, expected in cases:
        _admin_sql(tenzir, "DBCC FREEPROCCACHE")
        _run_pipeline(tenzir, f"{source}{pipeline}\nwrite_ndjson")
        queries = _cached_selects(tenzir, NUMBER_TABLE)
        if queries != [expected]:
            failures.append(f"{pipeline!r}: expected {expected!r}, got {queries}")


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    for ddl in (NUMBER_DDL, STRING_DDL, TIME_DDL):
        _admin_sql(tenzir, ddl)
    _admin_sql(tenzir, "DBCC FREEPROCCACHE")
    failures: list[str] = []
    try:
        _check(tenzir, NUMBER_TABLE, NUMBER_CASES, failures)
        _check(tenzir, STRING_TABLE, STRING_CASES, failures)
        _check(tenzir, TIME_TABLE, TIME_CASES, failures)
        _check_hints(tenzir, failures)
    finally:
        _admin_sql(
            tenzir, f"DROP TABLE IF EXISTS {NUMBER_TABLE}, {STRING_TABLE}, {TIME_TABLE}"
        )
    assert not failures, "\n".join(failures)
    total = len(NUMBER_CASES) + len(STRING_CASES) + len(TIME_CASES)
    print(f"ok ({total} predicates)")


if __name__ == "__main__":
    main()
