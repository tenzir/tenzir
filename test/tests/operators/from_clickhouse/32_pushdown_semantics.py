# runner: python
"""Pushed predicates select the same rows as their local evaluation.

Every predicate runs twice: in `table` mode, where it is translated into SQL,
and in `sql` mode, where it runs inside Tenzir on the same rows. The outputs
must match. The rows sit on the edges where TQL and ClickHouse could disagree:
NaN and infinities, integers at the 2^53 boundary, a `Float32` value that
`double` widens inexactly, out-of-range set elements, nulls, and quotes.

A second table covers the type families beyond numbers and strings: dates and
timestamps in every ClickHouse precision, including values past 2262 that TQL
cannot decode and a column in a non-UTC time zone; IPv4 and IPv6 addresses
against literals and subnets of either family; enums, UUIDs, and fixed strings
with their textual quirks; small-integer arithmetic; string functions; and
comparisons between two columns.

A third table stores IP addresses as strings, the way many ClickHouse tables
do. Here `table` mode adapts `ip` literals to the column type, so `s == 1.1.1.1`
matches the text `1.1.1.1`, and `s in 10.0.0.0/8` parses the column locally
behind a prefilter that ClickHouse evaluates with its own parser. `sql` mode
performs no adaptation, so these cases spell out the adapted predicate for the
local run. The rows collect spellings on which the two IP parsers might differ.

`system.query_log` confirms that the `table` mode query really carried the
predicate, so a silent fallback to local evaluation cannot make the test pass.
Each pipeline carries a second `where` on `id` with a bound unique to its
predicate. The bound never drops a row, but it is always pushed, so the logged
query can be attributed to its predicate even though the pipelines run
concurrently.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CONNECTION = (
    'host=env("CLICKHOUSE_HOST"),\n'
    '  port=int(env("CLICKHOUSE_PORT")),\n'
    '  password=env("CLICKHOUSE_PASSWORD"),\n'
    "  tls=false"
)

# (predicate, fragment that must appear in the pushed WHERE clause, or None
# when the predicate has no translation and must stay local)
Case = tuple[str, str | None]
# (predicate in `table` mode, fragment, predicate for the local run in `sql`
# mode when the adaptation changes it)
StringCase = tuple[str, str | None, str | None]
NUMBER_CASES: list[Case] = [
    # Integer column vs literals around 2^53.
    ("i > 0", "`i` > 0"),
    ("i == 4503599627370496", "`i` = 4503599627370496"),
    ("i == 4503599627370496.0", "`i` = 4503599627370496."),
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

UUID = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
DT3_GUARD = (
    "`dt3` >= fromUnixTimestamp64Milli(-9223372036854)"
    " AND `dt3` <= fromUnixTimestamp64Milli(9223372036854)"
)
TYPE_CASES: list[Case | StringCase] = [
    # `Date` against instants on a day, between days, and outside the range.
    ("d == 2024-01-01", "`d` = toDate('2024-01-01')"),
    ("d == 2024-01-01T12:00:00", "WHERE false"),
    ("d != 2024-01-01T12:00:00", "WHERE true"),
    ("d < 2024-01-01T12:00:00", "`d` <= toDate('2024-01-01')"),
    ("d >= 2024-01-01T12:00:00", "`d` > toDate('2024-01-01')"),
    ("d > 2150-01-01", "`d` > toDate('2149-06-06')"),
    ("d <= 2150-01-01", "`d` <= toDate('2149-06-06')"),
    ("d == 2150-01-01", "WHERE false"),
    ("d < 2024-01-01 - 100y", "`d` < toDate('1970-01-01')"),
    (
        "d in [2024-01-01, 2024-01-01T00:00:01, 2149-06-06]",
        "`d` IN (toDate('2024-01-01'), toDate('2149-06-06'))",
    ),
    ("d == d2", "`d` = `d2`"),
    ("d < d2", "`d` < `d2`"),
    # `Date32` holds days past 2262 that TQL decodes to `null`.
    (
        "d32 > 2000-01-01",
        "(`d32` <= toDate32('2262-04-11') AND `d32` > toDate32('2000-01-01'))",
    ),
    (
        "d32 <= 2262-04-11",
        "(`d32` <= toDate32('2262-04-11') AND `d32` <= toDate32('2262-04-11'))",
    ),
    ("d32 != 2000-01-01", "`d32` != toDate32('2000-01-01')"),
    ("d32 == 1900-01-01", "`d32` = toDate32('1900-01-01')"),
    ("d32 == null", "if(`d32` <= toDate32('2262-04-11'), `d32`, NULL) IS NULL"),
    ("d32 != null", "if(`d32` <= toDate32('2262-04-11'), `d32`, NULL) IS NOT NULL"),
    (
        "not (d32 > 2000-01-01)",
        "NOT if(`d32` <= toDate32('2262-04-11'), `d32` > toDate32('2000-01-01'), NULL)",
    ),
    ("not (d32 < 2000-01-01) or d32 == null", "NOT if("),
    ("d32 > 2000-01-01 or d == 1970-01-01", "((`d32` <= toDate32('2262-04-11') AND"),
    (
        "not (dt3 < 2024-01-01)",
        f"NOT if({DT3_GUARD}, `dt3` < fromUnixTimestamp64Milli(1704067200000), NULL)",
    ),
    ("not (dt3 > 2024-01-01)", "NOT if("),
    # `DateTime` in a non-UTC zone compares by instant.
    ("dt == 2024-01-01T12:34:56", "`dt` = toDateTime('2024-01-01 12:34:56', 'UTC')"),
    ("dt < 2024-01-01T12:34:56.5", "`dt` <= toDateTime('2024-01-01 12:34:56', 'UTC')"),
    ("dt >= 2107-01-01", "`dt` > toDateTime('2106-02-07 06:28:15', 'UTC')"),
    ("dt == 1970-01-01", "`dt` = toDateTime('1970-01-01 00:00:00', 'UTC')"),
    # `DateTime64(3)` holds instants past 2262 that TQL decodes to `null`.
    (
        "dt3 > 2024-01-01",
        f"({DT3_GUARD} AND `dt3` > fromUnixTimestamp64Milli(1704067200000))",
    ),
    (
        "dt3 < 2024-01-01",
        f"({DT3_GUARD} AND `dt3` < fromUnixTimestamp64Milli(1704067200000))",
    ),
    (
        "dt3 == 2024-01-01T00:00:00.123",
        "`dt3` = fromUnixTimestamp64Milli(1704067200123)",
    ),
    (
        "dt3 != 2024-01-01T00:00:00.123",
        "`dt3` != fromUnixTimestamp64Milli(1704067200123)",
    ),
    ("dt3 == 2024-01-01T00:00:00.1234", "WHERE false"),
    (
        "dt3 >= 2024-01-01T00:00:00.1234",
        f"({DT3_GUARD} AND `dt3` > fromUnixTimestamp64Milli(1704067200123))",
    ),
    ("dt3 == 1900-01-01", "`dt3` = fromUnixTimestamp64Milli(-2208988800000)"),
    ("dt3 == null", "IS NULL"),
    ("dt3 != null", "IS NOT NULL"),
    (
        "dt3 in [2024-01-01T00:00:00.123, 2024-01-01T00:00:00.1234]",
        "`dt3` IN (fromUnixTimestamp64Milli(1704067200123))",
    ),
    ("dt3 == dt3b", "IS NULL AND"),
    ("dt3 != dt3b", "NOT ("),
    ("dt3 < dt3b", "< if("),
    ("dt3 <= dt3b", "IS NULL) OR"),
    # `Nullable(DateTime64(2))` needs the generic literal form.
    (
        "dt2 == 2024-01-01T00:00:00.12",
        "(`dt2` IS NOT NULL AND `dt2` = CAST(fromUnixTimestamp64Nano(1704067200120000000), 'DateTime64(2)'))",
    ),
    ("dt2 != 2024-01-01T00:00:00.12", "(`dt2` IS NULL OR `dt2` != CAST("),
    (
        "dt2 < 2024-01-01",
        "AND `dt2` < CAST(fromUnixTimestamp64Nano(1704067200000000000), 'DateTime64(2)'))",
    ),
    ("dt2 == null", "IS NULL"),
    # `DateTime64(9)` decodes every tick.
    (
        "dt9 > 2024-01-01T00:00:00.000000001",
        "`dt9` > fromUnixTimestamp64Nano(1704067200000000001)",
    ),
    ("dt9 == 1900-01-01", "`dt9` = fromUnixTimestamp64Nano(-2208988800000000000)"),
    (
        "dt9 <= 2262-04-11T23:47:16.854775807",
        "`dt9` <= fromUnixTimestamp64Nano(9223372036854775807)",
    ),
    # Different temporal types stay local.
    ("d == d32", None),
    ("dt < dt3", None),
    ("dt3 == dt9", None),
    ('d == "2024-01-01"', None),
    # `IPv4` against addresses of either family, and subnets.
    ("ip4 == 10.0.0.1", "`ip4` = toIPv4('10.0.0.1')"),
    ("ip4 == ::ffff:10.0.0.1", "`ip4` = toIPv4('10.0.0.1')"),
    ("ip4 == ::1", "WHERE false"),
    ("ip4 != ::1", "WHERE true"),
    ("ip4 < ::1", "`ip4` < toIPv4('0.0.0.0')"),
    ("ip4 >= ::1", "`ip4` >= toIPv4('0.0.0.0')"),
    ("ip4 <= 2001:db8::", "`ip4` <= toIPv4('255.255.255.255')"),
    ("ip4 > 2001:db8::", "`ip4` > toIPv4('255.255.255.255')"),
    ("ip4 > 192.168.0.0", "`ip4` > toIPv4('192.168.0.0')"),
    (
        "ip4 in 10.0.0.0/8",
        "`ip4` BETWEEN toIPv4('10.0.0.0') AND toIPv4('10.255.255.255')",
    ),
    (
        "ip4 in 192.168.1.4/30",
        "`ip4` BETWEEN toIPv4('192.168.1.4') AND toIPv4('192.168.1.7')",
    ),
    ("ip4 in ::/0", "`ip4` BETWEEN toIPv4('0.0.0.0') AND toIPv4('255.255.255.255')"),
    (
        "ip4 in ::ffff:0:0/96",
        "`ip4` BETWEEN toIPv4('0.0.0.0') AND toIPv4('255.255.255.255')",
    ),
    ("ip4 in 2001:db8::/32", "`ip4` < toIPv4('0.0.0.0')"),
    (
        "ip4 in [10.0.0.1, ::1, 192.168.1.5]",
        "`ip4` IN (toIPv4('10.0.0.1'), toIPv4('192.168.1.5'))",
    ),
    ("ip4 in [::1]", "WHERE false"),
    # `Nullable(IPv6)` against addresses of either family, and subnets.
    ("ip6 == 10.0.0.1", "(`ip6` IS NOT NULL AND `ip6` = toIPv6('10.0.0.1'))"),
    ("ip6 == ::ffff:10.0.0.1", "(`ip6` IS NOT NULL AND `ip6` = toIPv6('10.0.0.1'))"),
    ("ip6 != ::1", "(`ip6` IS NULL OR `ip6` != toIPv6('::1'))"),
    ("ip6 < 10.0.0.1", "`ip6` < toIPv6('10.0.0.1')"),
    ("ip6 >= 2001:db8::", "`ip6` >= toIPv6('2001:db8::')"),
    (
        "ip6 in 10.0.0.0/8",
        "`ip6` BETWEEN toIPv6('10.0.0.0') AND toIPv6('10.255.255.255')",
    ),
    (
        "ip6 in ::ffff:0:0/96",
        "`ip6` BETWEEN toIPv6('0.0.0.0') AND toIPv6('255.255.255.255')",
    ),
    (
        "ip6 in 2001:db8::/32",
        "`ip6` BETWEEN toIPv6('2001:db8::') AND toIPv6('2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')",
    ),
    (
        "ip6 in ::/0",
        "`ip6` BETWEEN toIPv6('::') AND toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
    ),
    ("not (ip6 in 10.0.0.0/8)", "NOT `ip6` BETWEEN"),
    (
        "ip6 in [10.0.0.1, ::1]",
        "(`ip6` IS NOT NULL AND `ip6` IN (toIPv6('10.0.0.1'), toIPv6('::1')))",
    ),
    ("ip4 == ip6", "(`ip6` IS NOT NULL AND `ip4` = `ip6`)"),
    ("ip4 != ip6", "NOT (`ip6` IS NOT NULL AND `ip4` = `ip6`)"),
    ("ip4 < ip6", "`ip4` < `ip6`"),
    ("ip4 <= ip6", "`ip4` <= `ip6`"),
    ('ip4 == "10.0.0.1"', None),
    # A `String` column parses locally behind a prefilter; none of its values
    # here is an address.
    ("s in 10.0.0.0/8", "(toIPv6OrNull(`s`) IS NULL OR", "s.ip() in 10.0.0.0/8"),
    # Enums compare by name; an unknown name never matches.
    ('e == "high"', "`e` = 'high'"),
    ('e != "high"', "`e` != 'high'"),
    ('e == "nope"', "WHERE false"),
    ('e != "nope"', "WHERE true"),
    ('e in ["low", "nope"]', "`e` IN ('low')"),
    ('e in ["nope"]', "WHERE false"),
    ('e < "low"', None),
    ("e == e", None),
    # The decoder takes a label's rendering verbatim, backslashes included, so
    # TQL sees `a\\b` for the label `a\b`, and the literal is that rendering.
    ('e == "a\\\\\\\\b"', "`e` = 'a\\\\b'"),
    ('e == "a\\\\b"', "WHERE false"),
    ('e == "tab\\\\there"', "`e` = 'tab\\there'"),
    ('e == "tab\\there"', "WHERE false"),
    ('e in ["low", "a\\\\\\\\b"]', "`e` IN ('low', 'a\\\\b')"),
    # A tuple with an element that does not decode is absent locally, so its
    # elements are neither compared in SQL nor narrowed by a projection.
    ('half.ok == "x"', None),
    ('half.ok.starts_with("x")', None),
    # UUIDs match in canonical form only.
    (f'uid == "{UUID}"', f"`uid` = '{UUID}'"),
    (f'uid != "{UUID}"', f"`uid` != '{UUID}'"),
    (f'uid == "{UUID.upper()}"', "WHERE false"),
    ('uid == "not a uuid"', "WHERE false"),
    (f'uid in ["{UUID}", "{UUID.upper()}"]', f"`uid` IN ('{UUID}')"),
    (f'uid < "{UUID}"', None),
    ("uid == uid2", "`uid` = `uid2`"),
    ("uid != uid2", "`uid` != `uid2`"),
    ("uid < uid2", None),
    # Fixed strings keep their zero padding in TQL, so only a literal of the
    # full length can match.
    ('fs == "abcd"', "`fs` = 'abcd'"),
    ('fs != "abcd"', "`fs` != 'abcd'"),
    ('fs == "ab\\u0000\\u0000"', "`fs` = 'ab\\x00\\x00'"),
    ('fs == "ab"', "WHERE false"),
    ('fs != "ab"', "WHERE true"),
    ('fs == "abcde"', "WHERE false"),
    ('fs in ["ab", "abcd", "abcde"]', "`fs` IN ('abcd')"),
    ("fs.length_bytes() == 4", "length(`fs`) = 4"),
    ('fs < "b"', None),
    ("fs == s", None),
    # Strings order by bytes; functions with a direct counterpart translate.
    ('s < "b"', "`s` < 'b'"),
    ('s >= "b"', "`s` >= 'b'"),
    ('s > "z"', "`s` > 'z'"),
    ('s <= ""', "`s` <= ''"),
    ('"b" in s', "position(`s`, 'b') > 0"),
    ('"" in s', "position(`s`, '') > 0"),
    ('s.starts_with("a")', "startsWith(`s`, 'a')"),
    ('s.ends_with("c")', "endsWith(`s`, 'c')"),
    ('starts_with(s, "")', "startsWith(`s`, '')"),
    ("s.length_bytes() > 2", "length(`s`) > 2"),
    ("s.length_bytes() in [0, 2]", "length(`s`) IN (0, 2)"),
    ("s == t", "`s` = `t`"),
    ("s != t", "`s` != `t`"),
    ("s < t", "`s` < `t`"),
    ("s <= t", "`s` <= `t`"),
    ('s.starts_with("A", ignore_case=true)', None),
    ("s.length_chars() > 2", None),
    # Arithmetic on small integers and floats; overflow-prone forms stay local.
    ("i32 + 1 > 0", "(`i32` + 1) > 0"),
    ("i32 - 1 < 0", "(`i32` - 1) < 0"),
    ("i32 * -2 < 0", "(`i32` * -2) < 0"),
    ("i32 * 2147483647 > 0", "(`i32` * 2147483647) > 0"),
    ("i32 / 2 == 1.5", "(`i32` / 2) = 1.5"),
    # A `double` literal keeps its fraction so that arithmetic stays in
    # `Float64`; the row with `i64 = 2^53 + 1` rounds in TQL and must in SQL.
    ("i64 + 0.0 == 9007199254740992", "(`i64` + 0.) = 9007199254740992"),
    ("i64 * 1.0 == 9223372036854775807", None),
    ("u8 + 0.0 == 255", "(`u8` + 0.) = 255"),
    ("i32 * 2.0 > 3", "(`i32` * 2.) > 3"),
    ("i32 / 2 > 1", "(`i32` / 2) > 1"),
    ("1 + i32 == 4", "(1 + `i32`) = 4"),
    ("10 - i32 == 7", "(10 - `i32`) = 7"),
    ("u8 + 1 == 256", "(`u8` + 1) = 256"),
    ("u8 * 2 == 510", "(`u8` * 2) = 510"),
    ("u8 - -1 == 256", "(`u8` - -1) = 256"),
    ("u32 - -1 == 1", "((`u32` - -1) IS NOT NULL AND (`u32` - -1) = 1)"),
    ("5 - u32 < 0", "(5 - `u32`) < 0"),
    ("u32 / 2 > 1", "(`u32` / 2) > 1"),
    ("i32 + 0.5 > 3", "(`i32` + 0.5) > 3"),
    ("f + 1 > 1.1", "(`f` + 1) > 1.1"),
    ("f * 3 == 1.5", "(`f` * 3) = 1.5"),
    ("f / 0.5 >= 1", "(`f` / 0.5) >= 1"),
    ("2 - f > 1", "(2 - `f`) > 1"),
    ("i32 + 1 == 2147483648", "(`i32` + 1) = 2147483648"),
    ("i32 - 1 == -2147483649", "(`i32` - 1) = -2147483649"),
    ("u32 - 1 == 0", None),
    ("u32 + -1 == 0", None),
    ("u8 - 1 < 0", None),
    ("i32 + 2147483648 > 0", None),
    ("i32 + u8 > 0", None),
    ("i64 + 1 > 0", None),
    ("i32 / 0 > 1", None),
    ("1 / i32 > 1", None),
    # Literal expressions fold before translation.
    ("i32 > 1024 * 1024", "`i32` > 1048576"),
    ("i32 > -(1 + 2)", "`i32` > -3"),
    (
        "dt3 > 2024-01-01 - 1d",
        f"({DT3_GUARD} AND `dt3` > fromUnixTimestamp64Milli(1703980800000))",
    ),
    ("i32 > 9223372036854775807 + 1", None),
    ("dt3 > now() - 10y", None),
    # Comparisons between two integer columns, nullable or not.
    ("i32 < u32", "`i32` < `u32`"),
    ("i32 == u32", "(`u32` IS NOT NULL AND `i32` = `u32`)"),
    ("i32 != u32", "NOT (`u32` IS NOT NULL AND `i32` = `u32`)"),
    ("i32 <= u32", "`i32` <= `u32`"),
    (
        "u32 == n32",
        "((`u32` IS NULL AND `n32` IS NULL) OR (`u32` IS NOT NULL AND `n32` IS NOT NULL AND `u32` = `n32`))",
    ),
    ("u32 != n32", "NOT ((`u32` IS NULL AND `n32` IS NULL) OR"),
    ("u32 <= n32", "((`u32` IS NULL AND `n32` IS NULL) OR `u32` <= `n32`)"),
    ("u32 >= n32", "((`u32` IS NULL AND `n32` IS NULL) OR `u32` >= `n32`)"),
    ("u32 < n32", "`u32` < `n32`"),
    ("not (u32 <= n32)", "NOT ((`u32` IS NULL AND `n32` IS NULL) OR `u32` <= `n32`)"),
    ("i32 + 1 < u32", "(`i32` + 1) < `u32`"),
    ("i64 < u32", "`i64` < `u32`"),
    ("i32 < f", None),
]

S_PARSED = "toIPv6OrNull(`s`)"
S_IN_10 = (
    f"({S_PARSED} IS NULL OR {S_PARSED} BETWEEN toIPv6('10.0.0.0')"
    " AND toIPv6('10.255.255.255'))"
)
STRING_CASES: list[StringCase] = [
    # An `ip` literal against a `String` column compares against its text.
    ("s == 1.1.1.1", "`s` = '1.1.1.1'", 's == "1.1.1.1"'),
    ("1.1.1.1 == s", "`s` = '1.1.1.1'", 's == "1.1.1.1"'),
    ("s != 1.1.1.1", "`s` != '1.1.1.1'", 's != "1.1.1.1"'),
    ("s == 2001:DB8::1", "`s` = '2001:db8::1'", 's == "2001:db8::1"'),
    ("s == ::ffff:1.1.1.1", "`s` = '1.1.1.1'", 's == "1.1.1.1"'),
    (
        "s in [1.1.1.1, ::1, 10.0.0.1]",
        "`s` IN ('1.1.1.1', '::1', '10.0.0.1')",
        's in ["1.1.1.1", "::1", "10.0.0.1"]',
    ),
    ("ns == 1.1.1.1", "(`ns` IS NOT NULL AND `ns` = '1.1.1.1')", 'ns == "1.1.1.1"'),
    ("ns != 1.1.1.1", "(`ns` IS NULL OR `ns` != '1.1.1.1')", 'ns != "1.1.1.1"'),
    ("lc == 10.0.0.1", "`lc` = '10.0.0.1'", 'lc == "10.0.0.1"'),
    (
        "not (s == 1.1.1.1 or id > 20)",
        "NOT (`s` = '1.1.1.1' OR `id` > 20)",
        'not (s == "1.1.1.1" or id > 20)',
    ),
    # Ordering has no textual counterpart and keeps TQL's outcome: no rows.
    ("s < 1.1.1.1", None, None),
    # Membership in a subnet parses the column, with a prefilter in SQL.
    ("s in 10.0.0.0/8", S_IN_10, "s.ip() in 10.0.0.0/8"),
    ("s.ip() in 10.0.0.0/8", S_IN_10, None),
    (
        "ns in 10.0.0.0/8",
        "(toIPv6OrNull(`ns`) IS NULL OR toIPv6OrNull(`ns`) BETWEEN",
        "ns.ip() in 10.0.0.0/8",
    ),
    (
        "s in ::ffff:0:0/96",
        f"({S_PARSED} IS NULL OR {S_PARSED} BETWEEN toIPv6('0.0.0.0') AND toIPv6('255.255.255.255'))",
        "s.ip() in ::ffff:0:0/96",
    ),
    (
        "s in 2001:db8::/32",
        f"({S_PARSED} IS NULL OR {S_PARSED} BETWEEN toIPv6('2001:db8::') AND",
        "s.ip() in 2001:db8::/32",
    ),
    (
        "s in ::/0",
        f"({S_PARSED} IS NULL OR {S_PARSED} BETWEEN toIPv6('::') AND",
        "s.ip() in ::/0",
    ),
    (
        "s in 1.1.1.1/32",
        f"({S_PARSED} IS NULL OR {S_PARSED} BETWEEN toIPv6('1.1.1.1') AND toIPv6('1.1.1.1'))",
        "s.ip() in 1.1.1.1/32",
    ),
    # An explicit parse gets the same prefilter for equality and ordering.
    (
        "s.ip() == 1.1.1.1",
        f"({S_PARSED} IS NULL OR {S_PARSED} = toIPv6('1.1.1.1'))",
        None,
    ),
    ("s.ip() == ::1", f"({S_PARSED} IS NULL OR {S_PARSED} = toIPv6('::1'))", None),
    (
        "s.ip() in [1.1.1.1, ::1]",
        f"({S_PARSED} IS NULL OR {S_PARSED} IN (toIPv6('1.1.1.1'), toIPv6('::1')))",
        None,
    ),
    (
        "s.ip() < 10.0.0.0",
        f"({S_PARSED} IS NULL OR {S_PARSED} < toIPv6('10.0.0.0'))",
        None,
    ),
    (
        "s.ip() >= 2001:db8::",
        f"({S_PARSED} IS NULL OR {S_PARSED} >= toIPv6('2001:db8::'))",
        None,
    ),
    # `!=` and `not` keep rows TQL cannot parse, so they get no prefilter.
    ("s.ip() != 1.1.1.1", None, None),
    ("not (s in 10.0.0.0/8)", None, "not (s.ip() in 10.0.0.0/8)"),
    # Prefilters compose.
    (
        "s in 10.0.0.0/8 or s == 1.1.1.1",
        f"({S_IN_10} OR `s` = '1.1.1.1')",
        's.ip() in 10.0.0.0/8 or s == "1.1.1.1"',
    ),
    (
        "(s in 10.0.0.0/8 and s.length_chars() > 8) or id == 1",
        f"({S_IN_10} OR `id` = 1)",
        "(s.ip() in 10.0.0.0/8 and s.length_chars() > 8) or id == 1",
    ),
    (
        "s in 10.0.0.0/8 and id < 100",
        f"{S_IN_10} AND `id` < 100",
        "s.ip() in 10.0.0.0/8 and id < 100",
    ),
    (
        "s in 10.0.0.0/8 or s.length_chars() > 8",
        None,
        "s.ip() in 10.0.0.0/8 or s.length_chars() > 8",
    ),
    # A string literal is a plain string comparison, as before.
    ('s == "1.1.1.1"', "`s` = '1.1.1.1'", None),
    ('s in ["1.1.1.1", "::1"]', "`s` IN ('1.1.1.1', '::1')", None),
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


def _logged_selects(table: str) -> list[str]:
    """Returns the `SELECT` queries against `table`, oldest first."""
    _clickhouse("SYSTEM FLUSH LOGS")
    output = _clickhouse(
        "SELECT query FROM system.query_log"
        " WHERE type = 'QueryFinish' AND query_kind = 'Select'"
        f" AND has(tables, concat(currentDatabase(), '.{table}'))"
        " ORDER BY event_time_microseconds"
        " FORMAT TSVRaw"
    )
    return [line for line in output.splitlines() if line]


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


def _check(
    tenzir: tuple[str, ...],
    table: str,
    cases: list[Case] | list[StringCase] | list[Case | StringCase],
    failures: list[str],
) -> None:
    # A second `where` on `id` tags the query: its bound exceeds every id, so it
    # never drops a row, and it always has an exact translation.
    def tag(index: int) -> str:
        return f"`id` < {1000 + index}"

    def run(item: tuple[int, tuple]) -> tuple[str, str]:
        index, case = item
        predicate = case[0]
        local = case[2] if len(case) > 2 and case[2] else predicate
        tail = f"\nwhere id < {1000 + index}\nsort id\nwrite_ndjson"
        pushed = _run_pipeline(
            tenzir,
            f'from_clickhouse table="{table}",\n  {CONNECTION}\n'
            f"where {predicate}{tail}",
        )
        local = _run_pipeline(
            tenzir,
            f'from_clickhouse sql="SELECT * FROM {table}",\n  {CONNECTION}\n'
            f"where {local}{tail}",
        )
        return pushed, local

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(run, enumerate(cases)))
    queries = _logged_selects(table)
    for index, (case, (pushed, local)) in enumerate(zip(cases, results)):
        predicate, fragment = case[0], case[1]
        tagged = [q for q in queries if tag(index) in q]
        if not tagged:
            failures.append(f"{predicate!r}: no tagged query was logged")
            continue
        query = tagged[-1]
        if fragment is None:
            if query != f"SELECT * FROM {table} WHERE {tag(index)}":
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
