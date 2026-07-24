This release fixes two defects that could write corrupt data into persisted store files and keeps subscribers running when a publisher finishes. It also improves timestamp handling across the board, from datetime literals with timezone offsets to clearer warnings for unparsable values.

## 🔧 Changes

### Warning for unparsable timestamps in time()

The `time()` function now warns when it cannot make sense of a string instead of silently returning `null`. If your timestamps unexpectedly come out empty, the warning tells you which value failed to parse:

```tql
from {x: "not a timestamp"}
x = x.time()
```

```
warning: `time` failed to parse string
  = note: tried to convert: not a timestamp
```

*By @zedoraps and @claude in #6464.*

### Zero-offset timezone names in timestamp parsing

Timestamps ending in `GMT`, `UTC`, or `UT`—common in web server and mail logs—now parse out of the box, with or without a space before the name:

```tql
from {x: "2026-07-21 13:55:59.000 GMT"}
x = x.time()
```

Previously, such timestamps failed to parse because only `Z` and numeric offsets like `+02:00` were supported.

Note that this also affects automatic type detection: values like `"2026-07-21 13:55:59 GMT"` that previously stayed strings are now recognized as timestamps, for example when reading CSV files.

*By @zedoraps and @claude in #6464.*

## 🐞 Bug fixes

### Compactor no longer abandons continuation handlers spuriously

The compactor no longer logs spurious "*compactor abandons continuation handler for run N because it is not the current run anymore*" messages during temporal and spatial compaction runs.

These log messages were harmless overall, caused by a failure during compaction which did not properly stop the compaction run.

*By @IyeOnline in #6470.*

### Datetime literals with fractional seconds and timezone offset

Timestamps with both fractional seconds and a timezone offset can now be written directly in pipelines. Copying a timestamp like `2026-07-21T13:55:59.123+02:00` from a log into a filter just works:

```tql
where ts > 2026-07-21T13:55:59.123+02:00
```

Previously, this failed with a confusing syntax error, and you had to drop the fractional seconds or wrap the timestamp in a string and call `time()`.

*By @zedoraps and @claude in #6464.*

### Fixes for corrupt persisted store files

Tenzir nodes no longer risk writing stale memory or leftover file contents into persisted store files.

Two defects that could corrupt data written to disk are fixed:

- Overwriting an existing file with shorter content no longer leaves trailing bytes from the previous version. This could previously produce store files with garbage after the payload when a temporary file path was reused.
- An internal buffer handle that had been moved from still appeared valid and could hand out memory it no longer kept alive, allowing freed and reused memory to end up in files written to disk. Moved-from handles now behave like empty ones.

*By @tobim and @claude in #6471.*

### Subscribers no longer stop early when one publisher finishes

Subscribers to internal pub/sub topics (used by the `subscribe` and `publish` operators) no longer stop early just because one publisher for that topic finished. Previously, a `publish` pipeline completing normally could cause the node to signal "topic exhausted" to all subscribers of that topic, even while the node kept running and another publisher for the same topic could still start later. This could make a running `subscribe "topic"` pipeline terminate prematurely. Subscribers are now only told a topic is exhausted when the node itself is actually shutting down.

*By @IyeOnline in #6467.*

### Warnings for invalid ints now show the invalid value

Warnings from `int()` and `uint()` now show the invalid value:

```text
= note: tried to convert: invalid
```

*By @zedoraps and @claude in #6465.*
