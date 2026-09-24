---
title: Skip Parquet row groups that cannot match a filter
type: change
authors:
  - mavam
created: 2026-09-24T12:52:30.366374Z
---

`read_parquet` now skips row groups that cannot contain matching events. When a filter follows the reader, it compares the filter with the minimum and maximum value that the file records for each column of every row group, and reads only the row groups that may contain a match.

This makes it cheap to pull a narrow slice out of a large file, for example the last hour of a day of events sorted by time:

```tql
from_file "/data/events.parquet" {
  read_parquet
}
where time >= 2026-01-30T16:00:00Z
```

Skipping works for comparisons of a field with a constant, such as `==`, `!=`, `<`, and `>=`, and for combinations of them with `and` and `or`. It helps most when the filtered field is sorted or clustered, as timestamps usually are. Other filters still read every row group and produce the same results as before.
