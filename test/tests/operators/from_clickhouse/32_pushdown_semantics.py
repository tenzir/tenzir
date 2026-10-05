# runner: python
"""`from_clickhouse` selects the same rows whether or not it pushes a predicate.

Every predicate runs twice: in `table` mode, where `from_clickhouse` may push
it into the query, and in `sql` mode, where it runs inside Tenzir on the rows
of `SELECT *`. The two must select the same rows. The rows sit on the edges
where TQL and ClickHouse could disagree: NaN and infinities, integers at the
2^53 boundary, a `Float32` value that `double` widens inexactly, out-of-range
set elements, nulls, and quotes.

A second table covers the type families beyond numbers and strings: dates and
timestamps in every ClickHouse precision, including values past 2262 that TQL
cannot decode and a column in a non-UTC time zone; IPv4 and IPv6 addresses
against literals and subnets of either family; enums, UUIDs, and fixed strings
with their textual quirks; small-integer arithmetic; string functions; and
comparisons between two columns.

A third table stores IP addresses as strings, the way many ClickHouse tables
do. Here `table` mode adapts `ip` literals to the column type, so `s == 1.1.1.1`
matches the text `1.1.1.1`, and `s in 10.0.0.0/8` parses the column locally.
`sql` mode performs no adaptation, so these cases spell out the adapted
predicate for the local run. The rows collect spellings on which the two IP
parsers might differ.

What each predicate translates to is the subject of the unit tests in
`plugins/clickhouse/tests/`; this test only compares rows.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

CONNECTION = (
    'host=env("CLICKHOUSE_HOST"),\n'
    '  port=int(env("CLICKHOUSE_PORT")),\n'
    '  password=env("CLICKHOUSE_PASSWORD"),\n'
    "  tls=false"
)

# A predicate, or a predicate in `table` mode and its adapted spelling for the
# local run in `sql` mode.
Case = str | tuple[str, str]

# The predicates that share one `tenzir` process. Each runs as a subpipeline
# of `merge`, which pushes its filter into its own `from_clickhouse`.
BATCH = 64

NUMBER_CASES: list[Case] = [
    # Integer column vs literals around 2^53.
    "i > 0",
    "i == 4503599627370496",
    "i == 4503599627370496.0",
    "i == 9007199254740992.0",
    "i > 9007199254740992.0",
    "i in [9007199254740993, 0]",
    # A list with mixed literal types stays local: TQL nulls the clashing
    # elements, ClickHouse would match them.
    "i in [1, 0.0]",
    "i in [0.0, 1]",
    "i in [0, 18446744073709551615]",
    # UInt8 column vs out-of-range and negative literals.
    "u in [44, 300]",
    "u == 300",
    "u != 255",
    "u >= -1",
    # Float32 column: 0.1 widens inexactly, 16777217 is not representable.
    "f32 == 0.1",
    "f32 in [0.1, 0.5]",
    "f32 in [0.5, 16777216]",
    # Integer literals compare with floating-point columns as doubles.
    "f32 in [16777217]",
    "f32 == 16777217",
    "f32 >= 16777216",
    # Float64 column with nan and infinities.
    "f64 > 0",
    "f64 < 0",
    "f64 != 1",
    "f64 == 9007199254740992",
    "f64 == 9007199254740993",
    "f64 in [9007199254740992]",
    "not (f64 > 0)",
    # Nullable column: null-total equality, nan, ordering.
    "n == null",
    "n != null",
    "not (n == 1.5)",
    "n != 1.5",
    "n > 0",
    "n in [1.5, -1e300]",
    # Strings with quotes and backslashes.
    's == "it\'s"',
    's in ["b\\\\c", ""]',
    's != "a"',
    # Boolean combinators and a partially translatable conjunction.
    "i > 0 or f64 < 0",
    'i > 0 and s.starts_with("a")',
    'not (i > 0 and s == "a")',
]

UUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
TYPE_CASES: list[Case] = [
    # `Date` against instants on a day, between days, and outside the range.
    "d == 2024-01-01",
    "d == 2024-01-01T12:00:00",
    "d != 2024-01-01T12:00:00",
    "d < 2024-01-01T12:00:00",
    "d >= 2024-01-01T12:00:00",
    "d > 2150-01-01",
    "d <= 2150-01-01",
    "d == 2150-01-01",
    "d < 2024-01-01 - 100y",
    "d in [2024-01-01, 2024-01-01T00:00:01, 2149-06-06]",
    "d == d2",
    "d < d2",
    # `Date32` holds days past 2262 that TQL decodes to `null`.
    "d32 > 2000-01-01",
    "d32 <= 2262-04-11",
    "d32 != 2000-01-01",
    "d32 == 1900-01-01",
    "d32 == null",
    "d32 != null",
    "not (d32 > 2000-01-01)",
    "not (d32 < 2000-01-01) or d32 == null",
    "d32 > 2000-01-01 or d == 1970-01-01",
    "not (dt3 < 2024-01-01)",
    "not (dt3 > 2024-01-01)",
    # `DateTime` in a non-UTC zone compares by instant.
    "dt == 2024-01-01T12:34:56",
    "dt < 2024-01-01T12:34:56.5",
    "dt >= 2107-01-01",
    "dt == 1970-01-01",
    # `DateTime64(3)` holds instants past 2262 that TQL decodes to `null`.
    "dt3 > 2024-01-01",
    "dt3 < 2024-01-01",
    "dt3 == 2024-01-01T00:00:00.123",
    "dt3 != 2024-01-01T00:00:00.123",
    "dt3 == 2024-01-01T00:00:00.1234",
    "dt3 >= 2024-01-01T00:00:00.1234",
    "dt3 == 1900-01-01",
    "dt3 == null",
    "dt3 != null",
    "dt3 in [2024-01-01T00:00:00.123, 2024-01-01T00:00:00.1234]",
    "dt3 == dt3b",
    "dt3 != dt3b",
    "dt3 < dt3b",
    "dt3 <= dt3b",
    # `Nullable(DateTime64(2))` needs the generic literal form.
    "dt2 == 2024-01-01T00:00:00.12",
    "dt2 != 2024-01-01T00:00:00.12",
    "dt2 < 2024-01-01",
    "dt2 == null",
    # `DateTime64(9)` decodes every tick.
    "dt9 > 2024-01-01T00:00:00.000000001",
    "dt9 == 1900-01-01",
    "dt9 <= 2262-04-11T23:47:16.854775807",
    # Different temporal types stay local.
    "d == d32",
    "dt < dt3",
    "dt3 == dt9",
    'd == "2024-01-01"',
    # `IPv4` against addresses of either family, and subnets.
    "ip4 == 10.0.0.1",
    "ip4 == ::ffff:10.0.0.1",
    "ip4 == ::1",
    "ip4 != ::1",
    "ip4 < ::1",
    "ip4 >= ::1",
    "ip4 <= 2001:db8::",
    "ip4 > 2001:db8::",
    "ip4 > 192.168.0.0",
    "ip4 in 10.0.0.0/8",
    "ip4 in 192.168.1.4/30",
    "ip4 in ::/0",
    "ip4 in ::ffff:0:0/96",
    "ip4 in 2001:db8::/32",
    "ip4 in [10.0.0.1, ::1, 192.168.1.5]",
    "ip4 in [::1]",
    # `Nullable(IPv6)` against addresses of either family, and subnets.
    "ip6 == 10.0.0.1",
    "ip6 == ::ffff:10.0.0.1",
    "ip6 != ::1",
    "ip6 < 10.0.0.1",
    "ip6 >= 2001:db8::",
    "ip6 in 10.0.0.0/8",
    "ip6 in ::ffff:0:0/96",
    "ip6 in 2001:db8::/32",
    "ip6 in ::/0",
    "not (ip6 in 10.0.0.0/8)",
    "ip6 in [10.0.0.1, ::1]",
    "ip4 == ip6",
    "ip4 != ip6",
    "ip4 < ip6",
    "ip4 <= ip6",
    'ip4 == "10.0.0.1"',
    # A `String` column parses locally behind a prefilter; none of its values
    # here is an address.
    ("s in 10.0.0.0/8", "s.ip() in 10.0.0.0/8"),
    # Enums compare by name; an unknown name never matches.
    'e == "high"',
    'e != "high"',
    'e == "nope"',
    'e != "nope"',
    'e in ["low", "nope"]',
    'e in ["nope"]',
    'e < "low"',
    "e == e",
    # The decoder takes a label's rendering verbatim, backslashes included, so
    # TQL sees `a\\b` for the label `a\b`, and the literal is that rendering.
    'e == "a\\\\\\\\b"',
    'e == "a\\\\b"',
    'e == "tab\\\\there"',
    'e == "tab\\there"',
    'e in ["low", "a\\\\\\\\b"]',
    # A tuple with an element that does not decode is absent locally, so its
    # elements are neither compared in SQL nor narrowed by a projection.
    'half.ok == "x"',
    'half.ok.starts_with("x")',
    # UUIDs match in canonical form only.
    f'uid == "{UUID}"',
    f'uid != "{UUID}"',
    f'uid == "{UUID.upper()}"',
    'uid == "not a uuid"',
    f'uid in ["{UUID}", "{UUID.upper()}"]',
    f'uid < "{UUID}"',
    "uid == uid2",
    "uid != uid2",
    "uid < uid2",
    # Fixed strings keep their zero padding in TQL, so only a literal of the
    # full length can match.
    'fs == "abcd"',
    'fs != "abcd"',
    'fs == "ab\\u0000\\u0000"',
    'fs == "ab"',
    'fs != "ab"',
    'fs == "abcde"',
    'fs in ["ab", "abcd", "abcde"]',
    "fs.length_bytes() == 4",
    'fs < "b"',
    "fs == s",
    # Strings order by bytes; functions with a direct counterpart translate.
    's < "b"',
    's >= "b"',
    's > "z"',
    's <= ""',
    '"b" in s',
    '"" in s',
    's.starts_with("a")',
    's.ends_with("c")',
    'starts_with(s, "")',
    "s.length_bytes() > 2",
    "s.length_bytes() in [0, 2]",
    "s == t",
    "s != t",
    "s < t",
    "s <= t",
    's.starts_with("A", ignore_case=true)',
    't.ends_with("É", ignore_case=true)',
    "s.length_chars() > 2",
    # Regular expressions search with RE2 on both sides. ClickHouse lets `.`
    # match a newline unless the pattern starts with `(?-s)`.
    's.match_regex("^a.c")',
    's.match_regex("(?i)B")',
    's.match_regex("^.$")',
    's.match_regex("")',
    'not t.match_regex("c$")',
    # Arithmetic on small integers and floats; overflow-prone forms stay local.
    "i32 + 1 > 0",
    "i32 - 1 < 0",
    "i32 * -2 < 0",
    "i32 * 2147483647 > 0",
    "i32 / 2 == 1.5",
    # A `double` literal keeps its fraction so that arithmetic stays in
    # `Float64`; the row with `i64 = 2^53 + 1` rounds in TQL and must in SQL.
    "i64 + 0.0 == 9007199254740992",
    "i64 * 1.0 == 9223372036854775807",
    "u8 + 0.0 == 255",
    "i32 * 2.0 > 3",
    "i32 / 2 > 1",
    "1 + i32 == 4",
    "10 - i32 == 7",
    "u8 + 1 == 256",
    "u8 * 2 == 510",
    "u8 - -1 == 256",
    "u32 - -1 == 1",
    "5 - u32 < 0",
    "u32 / 2 > 1",
    "i32 + 0.5 > 3",
    "f + 1 > 1.1",
    "f * 3 == 1.5",
    "f / 0.5 >= 1",
    "2 - f > 1",
    "i32 + 1 == 2147483648",
    "i32 - 1 == -2147483649",
    "u32 - 1 == 0",
    "u32 + -1 == 0",
    "u8 - 1 < 0",
    "i32 + 2147483648 > 0",
    "i32 + u8 > 0",
    "i64 + 1 > 0",
    "i32 / 0 > 1",
    "1 / i32 > 1",
    # Literal expressions fold before translation.
    "i32 > 1024 * 1024",
    "i32 > -(1 + 2)",
    "dt3 > 2024-01-01 - 1d",
    "i32 > 9223372036854775807 + 1",
    "dt3 > now() - 10y",
    # Comparisons between two integer columns, nullable or not.
    "i32 < u32",
    "i32 == u32",
    "i32 != u32",
    "i32 <= u32",
    "u32 == n32",
    "u32 != n32",
    "u32 <= n32",
    "u32 >= n32",
    "u32 < n32",
    "not (u32 <= n32)",
    "i32 + 1 < u32",
    "i64 < u32",
    "i32 < f",
]


STRING_CASES: list[Case] = [
    # An `ip` literal against a `String` column compares against its text.
    ("s == 1.1.1.1", 's == "1.1.1.1"'),
    ("1.1.1.1 == s", 's == "1.1.1.1"'),
    ("s != 1.1.1.1", 's != "1.1.1.1"'),
    ("s == 2001:DB8::1", 's == "2001:db8::1"'),
    ("s == ::ffff:1.1.1.1", 's == "1.1.1.1"'),
    ("s in [1.1.1.1, ::1, 10.0.0.1]", 's in ["1.1.1.1", "::1", "10.0.0.1"]'),
    ("ns == 1.1.1.1", 'ns == "1.1.1.1"'),
    ("ns != 1.1.1.1", 'ns != "1.1.1.1"'),
    ("lc == 10.0.0.1", 'lc == "10.0.0.1"'),
    ("not (s == 1.1.1.1 or id > 20)", 'not (s == "1.1.1.1" or id > 20)'),
    # Ordering has no textual counterpart and keeps TQL's outcome: no rows.
    "s < 1.1.1.1",
    # Membership in a subnet parses the column, with a prefilter in SQL.
    ("s in 10.0.0.0/8", "s.ip() in 10.0.0.0/8"),
    "s.ip() in 10.0.0.0/8",
    ("ns in 10.0.0.0/8", "ns.ip() in 10.0.0.0/8"),
    ("s in ::ffff:0:0/96", "s.ip() in ::ffff:0:0/96"),
    ("s in 2001:db8::/32", "s.ip() in 2001:db8::/32"),
    ("s in ::/0", "s.ip() in ::/0"),
    ("s in 1.1.1.1/32", "s.ip() in 1.1.1.1/32"),
    # An explicit parse gets the same prefilter for equality and ordering.
    "s.ip() == 1.1.1.1",
    "s.ip() == ::1",
    "s.ip() in [1.1.1.1, ::1]",
    "s.ip() < 10.0.0.0",
    "s.ip() >= 2001:db8::",
    # `!=` and `not` keep rows TQL cannot parse, so they get no prefilter.
    "s.ip() != 1.1.1.1",
    ("not (s in 10.0.0.0/8)", "not (s.ip() in 10.0.0.0/8)"),
    # Prefilters compose.
    ("s in 10.0.0.0/8 or s == 1.1.1.1", 's.ip() in 10.0.0.0/8 or s == "1.1.1.1"'),
    (
        "(s in 10.0.0.0/8 and s.length_chars() > 8) or id == 1",
        "(s.ip() in 10.0.0.0/8 and s.length_chars() > 8) or id == 1",
    ),
    ("s in 10.0.0.0/8 and id < 100", "s.ip() in 10.0.0.0/8 and id < 100"),
    (
        "s in 10.0.0.0/8 or s.length_chars() > 8",
        "s.ip() in 10.0.0.0/8 or s.length_chars() > 8",
    ),
    # Without `(?-s)`, `.` would match the trailing newline of row 32, and
    # `$` matches only at the very end, as in RE2.
    'lc.match_regex("^1.1.1.1.")',
    'lc.match_regex("(?s)^1.1.1.1.")',
    'lc.match_regex("^1.1.1.1$")',
    's.match_regex("^\\\\d+\\\\.")',
    # A string literal is a plain string comparison, as before.
    's == "1.1.1.1"',
    's in ["1.1.1.1", "::1"]',
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
        timeout=60,
    )
    return result.stdout.strip()


NUMBER_TABLE = "fc_pushdown_semantics"
NUMBER_DDL = f"""
DROP TABLE IF EXISTS {NUMBER_TABLE};
CREATE TABLE {NUMBER_TABLE} (
  id UInt64,
  i Int64,
  u UInt8,
  f32 Float32,
  f64 Float64,
  n Nullable(Float64),
  s String
) ENGINE = MergeTree ORDER BY id;
INSERT INTO {NUMBER_TABLE} VALUES
  (1, 9007199254740993, 44, 16777216, 9007199254740992, NULL, 'a'),
  (2, -9007199254740993, 255, 0.1, nan, nan, 'it''s'),
  (3, 0, 0, -0.0, inf, 1.5, 'b\\\\c'),
  (4, 4503599627370496, 1, 0.5, -inf, -1e300, ''),
  (5, 9007199254740992, 2, 16777218, 0, 0, 'a');
"""

TYPE_TABLE = "fc_pushdown_semantics_types"
# Temporal columns take their values as instants, not as strings, so that the
# `Asia/Tokyo` column and the `DateTime64` columns hold exactly the intended
# points in time. Row 3 holds a `Date32` and a `DateTime64(3)` in 2299, which
# TQL cannot represent and decodes to `null`; row 4 holds the last decodable
# instants. `DateTime64(9)` ranges over the whole `int64`.
TYPE_DDL = f"""
DROP TABLE IF EXISTS {TYPE_TABLE};
CREATE TABLE {TYPE_TABLE} (
  id UInt64,
  d Date,
  d2 Date,
  d32 Date32,
  dt DateTime('Asia/Tokyo'),
  dt2 Nullable(DateTime64(2)),
  dt3 DateTime64(3),
  dt3b DateTime64(3, 'Asia/Tokyo'),
  dt9 DateTime64(9),
  ip4 IPv4,
  ip6 Nullable(IPv6),
  e Enum8('low' = 1, 'high' = 2, 'a\\\\b' = 3, 'tab\\there' = 4),
  half Tuple(ok String, bad Map(String, Int64)),
  uid UUID,
  uid2 UUID,
  fs FixedString(4),
  s String,
  t LowCardinality(String),
  i32 Int32,
  u32 Nullable(UInt32),
  n32 Nullable(Int32),
  u8 UInt8,
  i64 Int64,
  f Float32
) ENGINE = MergeTree ORDER BY id;
INSERT INTO {TYPE_TABLE} SELECT
  1 AS id, toDate('2024-01-01') AS d, toDate('2024-01-01') AS d2, toDate32('2024-01-01') AS d32,
  toDateTime(1704112496) AS dt, NULL AS dt2,
  fromUnixTimestamp64Milli(1704067200123) AS dt3, fromUnixTimestamp64Milli(1704067200123) AS dt3b,
  fromUnixTimestamp64Nano(1704067200000000001) AS dt9,
  toIPv4('10.0.0.1') AS ip4, NULL AS ip6, 'low' AS e, ('x', map('k', 1)) AS half, toUUID('{UUID}') AS uid, toUUID('{UUID}') AS uid2,
  'ab' AS fs, 'abc' AS s, 'abc' AS t, -2147483648 AS i32, NULL AS u32, NULL AS n32, 255 AS u8,
  9223372036854775807 AS i64, 0.1 AS f
UNION ALL SELECT
  2, toDate('1970-01-01'), toDate('1970-01-02'), toDate32('1900-01-01'),
  toDateTime(0), CAST(fromUnixTimestamp64Milli(1704067200120), 'DateTime64(2)'),
  fromUnixTimestamp64Milli(-2208988800000), fromUnixTimestamp64Milli(10413791999999),
  fromUnixTimestamp64Nano(-2208988800000000000),
  toIPv4('192.168.1.5'), toIPv6('::1'), 'high', ('y', map('k', 2)), toUUID('{UUID}'), generateUUIDv4(),
  'abcd', 'b', 'abc', 0, 0, NULL, 0, -9223372036854775807, 1
UNION ALL SELECT
  3, toDate('2149-06-06'), toDate('2149-06-06'), toDate32('2299-12-31'),
  toDateTime(4294967295), CAST(fromUnixTimestamp64Milli(1704067200000), 'DateTime64(2)'),
  fromUnixTimestamp64Milli(10413791999999), fromUnixTimestamp64Milli(10413791999999),
  fromUnixTimestamp64Nano(9223372036854775807),
  toIPv4('0.0.0.0'), toIPv6('10.0.0.1'), 'a\\\\b', ('x', map()), generateUUIDv4(), generateUUIDv4(),
  'a', '', 'b', 3, 4294967295, 3, 1, 0, -0.0
UNION ALL SELECT
  4, toDate('2024-01-02'), toDate('2000-01-01'), toDate32('2262-04-11'),
  toDateTime(1704112497), CAST(fromUnixTimestamp64Milli(-2208988800000), 'DateTime64(2)'),
  fromUnixTimestamp64Milli(9223372036854), fromUnixTimestamp64Milli(1704067200000),
  fromUnixTimestamp64Nano(-9223372036854775808),
  toIPv4('255.255.255.255'), toIPv6('2001:db8::1'), 'tab\\there', ('z', map()), generateUUIDv4(), generateUUIDv4(),
  'abc', 'é', 'é', 2147483647, 3, 0, 2, 1, 16777217
UNION ALL SELECT
  5, toDate('2000-06-15'), toDate('2000-06-15'), toDate32('2000-06-15'),
  toDateTime(946684800), CAST(fromUnixTimestamp64Milli(9223372036854), 'DateTime64(2)'),
  fromUnixTimestamp64Milli(1704067200000), fromUnixTimestamp64Milli(-2208988800000),
  fromUnixTimestamp64Nano(0),
  toIPv4('10.255.255.255'), toIPv6('10.255.255.255'), 'low', ('x', map()), generateUUIDv4(), generateUUIDv4(),
  'zzzz', 'abcabc', 'abcd', 7, NULL, 7, 3, 9007199254740993, 0.5;
"""

STRING_TABLE = "fc_pushdown_semantics_strings"
# Spellings that one IP parser may accept and the other reject, or both parse
# to the same address, plus plain non-addresses.
STRING_DDL = f"""
DROP TABLE IF EXISTS {STRING_TABLE};
CREATE TABLE {STRING_TABLE} (
  id UInt64,
  s String,
  ns Nullable(String),
  lc LowCardinality(String)
) ENGINE = MergeTree ORDER BY id;
INSERT INTO {STRING_TABLE} VALUES
  (1, '1.1.1.1', '1.1.1.1', '1.1.1.1'),
  (2, '01.1.1.1', NULL, '01.1.1.1'),
  (3, '::ffff:1.1.1.1', '::ffff:1.1.1.1', '::ffff:1.1.1.1'),
  (4, '::FFFF:1.1.1.1', '::FFFF:1.1.1.1', ''),
  (5, '2001:DB8::1', '2001:DB8::1', '2001:db8::1'),
  (6, '2001:db8::1', '2001:db8::1', '10.0.0.1'),
  (7, '2001:0db8:0000:0000:0000:0000:0000:0001', NULL, '10.0.0.1'),
  (8, '::1.2.3.4', '::1.2.3.4', '10.0.0.1'),
  (9, '10.0.0.1', '10.0.0.1', '10.0.0.1'),
  (10, '10.255.255.255', '10.255.255.255', '10.255.255.255'),
  (11, '11.0.0.0', '11.0.0.0', '11.0.0.0'),
  (12, '010.0.0.1', '010.0.0.1', '010.0.0.1'),
  (13, '10.0.0.1 ', ' 10.0.0.1', '10.0.0.1 '),
  (14, 'garbage', 'garbage', 'garbage'),
  (15, '', '', ''),
  (16, 'fe80::1%eth0', 'fe80::1%eth0', 'fe80::1%eth0'),
  (17, '[::1]', '[::1]', '[::1]'),
  (18, '::1', '::1', '::1'),
  (19, '256.1.1.1', '256.1.1.1', '256.1.1.1'),
  (20, '10.0.0.1/24', '10.0.0.1/24', '10.0.0.1/24'),
  (21, '167772161', '167772161', '167772161'),
  (22, '0x0a000001', '0x0a000001', '0x0a000001'),
  (23, '10.0.1', '10.0.1', '10.0.1'),
  (24, '10.0.0.1.1', '10.0.0.1.1', '10.0.0.1.1'),
  (25, '::', '::', '::'),
  (26, '0.0.0.0', '0.0.0.0', '0.0.0.0'),
  (27, '255.255.255.255', '255.255.255.255', '255.255.255.255'),
  (28, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', NULL, 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
  (29, '2001:db8:0:0:0:0:0:1', '2001:db8:0:0:0:0:0:1', '2001:db8:0:0:0:0:0:1'),
  (30, '::ffff:10.0.0.1', '::ffff:10.0.0.1', '::ffff:10.0.0.1'),
  (31, '::10.0.0.1', '::10.0.0.1', '::10.0.0.1'),
  (32, '0a.0.0.1', '1.1.1.01', '1.1.1.1\\n');
"""


def _leg(table: str, index: int, predicate: str, pushed: bool) -> str:
    source = f'table="{table}"' if pushed else f'sql="SELECT * FROM {table}"'
    return (
        f"from_clickhouse {source},\n  {CONNECTION}\n"
        f"where {predicate}\n"
        f"select id, case={index}, pushed={str(pushed).lower()}"
    )


def _run_batch(
    tenzir: tuple[str, ...], table: str, batch: list[tuple[int, Case]]
) -> dict[tuple[int, bool], list[int]]:
    """Returns the ids that each case selects, keyed by case and mode."""
    legs = []
    for index, case in batch:
        predicate, local = (case, case) if isinstance(case, str) else case
        legs.append(_leg(table, index, predicate, True))
        legs.append(_leg(table, index, local, False))
    pipeline = legs[0]
    for leg in legs[1:]:
        pipeline += "\nmerge {\n  " + leg.replace("\n", "\n  ") + "\n}"
    pipeline += "\nwrite_ndjson"
    with tempfile.TemporaryDirectory(prefix="fc-semantics-") as tmpdir:
        pipe_path = Path(tmpdir) / "pipe.tql"
        pipe_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--nova=true",
                "--bare-mode",
                "--console-verbosity=error",
                "-f",
                str(pipe_path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env=os.environ.copy(),
        )
    assert result.returncode == 0, (
        f"pipeline failed: rc={result.returncode}\n{pipeline}\n{result.stderr}"
    )
    ids: dict[tuple[int, bool], list[int]] = {}
    for line in result.stdout.splitlines():
        row = json.loads(line)
        ids.setdefault((row["case"], row["pushed"]), []).append(row["id"])
    return ids


def _check(
    tenzir: tuple[str, ...], table: str, cases: list[Case], failures: list[str]
) -> None:
    indexed = list(enumerate(cases))
    for start in range(0, len(indexed), BATCH):
        batch = indexed[start : start + BATCH]
        ids = _run_batch(tenzir, table, batch)
        for index, case in batch:
            pushed = sorted(ids.get((index, True), []))
            local = sorted(ids.get((index, False), []))
            if pushed != local:
                predicate = case if isinstance(case, str) else case[0]
                failures.append(
                    f"{table}: {predicate!r} selects {pushed} when pushed, "
                    f"{local} locally"
                )


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    _clickhouse(NUMBER_DDL)
    _clickhouse(TYPE_DDL)
    _clickhouse(STRING_DDL)
    failures: list[str] = []
    _check(tenzir, NUMBER_TABLE, NUMBER_CASES, failures)
    _check(tenzir, TYPE_TABLE, TYPE_CASES, failures)
    _check(tenzir, STRING_TABLE, STRING_CASES, failures)
    assert not failures, "\n".join(failures)
    total = len(NUMBER_CASES) + len(TYPE_CASES) + len(STRING_CASES)
    print(f"ok ({total} predicates)")


if __name__ == "__main__":
    main()
