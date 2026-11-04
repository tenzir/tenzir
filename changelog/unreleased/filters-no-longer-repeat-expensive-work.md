---
title: Filters no longer repeat expensive work
type: bugfix
authors:
  - jachris
created: 2026-11-04T00:04:32.59126Z
---

Filters that read a computed field, such as `where s.app_name == "sshd"` after
`s = line.parse_syslog()`, no longer slow down the pipeline. Previously, the
optimizer moved such filters in front of the assignment by copying the
computation into the filter, so the pipeline parsed every event once more, and
once per field that the filter read. Chained assignments could multiply the
copies, which made some pipelines many times slower. The same applied to filters
inside `if` and `match` branches whose condition was expensive. Such filters now
stay right behind the expensive operation, while filters on plain fields and
cheap conversions still move towards the source.
