---
title: Reference date for partial parse_time formats
type: feature
authors:
  - Zedoraps
  - codex
prs:
  - 6380
created: 2026-06-23T00:00:00.000000Z
---

The `parse_time` function now accepts a `reference` argument for formats that
don't include a complete date. Parsed date fields win, and missing year, month,
and day fields come from `reference`:

```tql
timestamp = timestamp.parse_time("%b %e %H:%M:%S", reference=now())
```

When the input contains month and day but no year, `parse_time` chooses the year
that places the parsed timestamp closest to the reference time.

Without `reference`, year-less formats keep the previous default year of 1970
for now, but emit a deprecation warning because this will become an error in a
future release. If `reference` is null and a date field is missing, the result
is null and `parse_time` emits a warning. Formats that include a complete date
warn when `reference` is set.
