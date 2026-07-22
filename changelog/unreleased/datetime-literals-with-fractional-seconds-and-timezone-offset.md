---
title: Datetime literals with fractional seconds and timezone offset
type: bugfix
authors:
  - zedoraps
  - claude
prs:
  - 6464
created: 2026-07-22T12:15:48.639591Z
---

Timestamps with both fractional seconds and a timezone offset can now be
written directly in pipelines. Copying a timestamp like
`2026-07-21T13:55:59.123+02:00` from a log into a filter just works:

```tql
where ts > 2026-07-21T13:55:59.123+02:00
```

Previously, this failed with a confusing syntax error, and you had to drop
the fractional seconds or wrap the timestamp in a string and call `time()`.
