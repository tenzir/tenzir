# Timestamp prefilter validation

The timestamp cases in `kql_observables.py` and the paired
`pushdown-time-*.tql` tests exercise query-output timestamps for Defender and
Log Analytics. Fixtures assert requests and emulate candidate rows; they do not
execute KQL. The local-only cases return every fixture row and use an optimizer
barrier before the same predicate.

## Live characterization

On 2026-10-20, twelve synthetic queries ran against each of the Log Analytics
query API and Microsoft Graph v1.0 `security/runHuntingQuery`. All 24 returned
HTTP 200. No customer tables were queried. Authentication used separate tokens
for the two service audiences, and HTTPS certificate validation was enabled.

Both APIs returned seven fractional digits unchanged. Log Analytics encoded a
typed datetime null as JSON `null`; Graph encoded it as `{}`. Both existing
connectors decode those values to TQL `null`.

The following query reproduces the live timestamp comparison dataset. The
fixtures replace rows 9 and 10 with 1900-01-01 and 2262-01-01, so decoder
overflow is not a prerequisite for passing the tests. The calendar parser
rejects dates before 1900; the historical fixture uses that earliest year. Fixture row 11 additionally covers the instant before the Unix epoch:

```kusto
datatable(Id:long, TimeGenerated:datetime, RemotePort:long) [
  1, datetime(2026-10-19T10:02:59.9999999Z), 443,
  2, datetime(2026-10-19T10:03:00Z), 443,
  3, datetime(2026-10-19T10:03:00.1234566Z), 443,
  4, datetime(2026-10-19T10:03:00.1234567Z), 443,
  5, datetime(2026-10-19T10:03:00.1234568Z), 443,
  6, datetime(2026-10-19T10:03:00.123459Z), 443,
  7, datetime(2026-10-19T10:08:00Z), 443,
  8, datetime(null), 443,
  9, datetime(1600-01-01), 443,
  10, datetime(2500-01-01), 443
]
| sort by Id asc
```

Append this equality prefilter:

```kusto
| where (gettype(column_ifexists('TimeGenerated', dynamic(null))) != "datetime"
  or todatetime(column_ifexists('TimeGenerated', dynamic(null))) < datetime(1678-01-01)
  or todatetime(column_ifexists('TimeGenerated', dynamic(null))) >= datetime(2262-01-01)
  or (todatetime(column_ifexists('TimeGenerated', dynamic(null))) >= datetime(2026-10-19T10:03:00.123455Z)
    and todatetime(column_ifexists('TimeGenerated', dynamic(null))) <= datetime(2026-10-19T10:03:00.123458Z)))
```

Both services returned these candidate IDs:

| Comparison against `2026-10-19T10:03:00.1234567Z` | Candidate IDs |
| --- | --- |
| `==` (the interval shown) | 3, 4, 5, 9, 10 |
| `<` or `<=` (upper bound only) | 1, 2, 3, 4, 5, 9, 10 |
| `>` or `>=` (lower bound only) | 3, 4, 5, 6, 7, 9, 10 |

Additional queries replaced the timestamp column with a string, a dynamic
string, a long, or a boolean, or omitted it. The guarded predicate compiled and
retained the row in each case, on both services. A query with `take 5` followed
by `summarize TimeGenerated=max(TimeGenerated) by RemotePort` retained the
aggregate timestamp `.1234568` when the equality prefilter was appended.

## Why the bounds are conservative

The shared translation carries a decoded TQL instant, not a source-specific
native temporal literal. Only the KQL renderer accepts that literal for a
dynamic-column comparison; SQL renderers veto it.

KQL stores 100-nanosecond ticks. Both connector readers parse UTC timestamps
through floating-point seconds since midnight, then truncate to integer
nanoseconds. This can shift a timestamp by a nanosecond. Each bound therefore
widens by one microsecond and rounds outwards to microseconds. Those bounds
are exactly representable in KQL. Equality becomes an interval; strict and
inclusive comparisons use the same candidate bound. Exact evaluation remains
local.

The predicate retains non-datetime types and calendar years outside
`[1900, 2262)`. Years before 1900 are rejected by the calendar parser and must
remain candidates so pushdown does not hide decoder errors. The paired
`pushdown-time-pre-1900` tests verify that both connectors still reject an
1899 timestamp. Calendar conversion beyond the nanosecond range can overflow;
the prefilter must not assume those values decode monotonically. The partial
upper boundary year is also retained deliberately. The live probe above used
the original 1678 guard; widening it to 1900 only retains additional candidates.

Negation, timestamp inequality, timestamp list membership, nested field paths,
column-to-column comparisons, and timestamp arithmetic in downstream filters
remain local. An `or` with an unsupported branch stays local as a whole.

The planner still appends predicates after the unchanged user query, keeps
local-filter columns in projections, applies Defender's result cap before
prefiltering, and keeps downstream limits after the exact filter. It never
narrows the API request window.
