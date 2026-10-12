---
title: Clean shutdown of live MySQL and SQL Server reads
type: bugfix
authors:
  - mavam
created: 2026-10-03T05:24:17.83035Z
---

Stopping a pipeline that reads with `from_mysql live=true` or `from_microsoft_sql live=true`, for example with Ctrl+C or a node shutdown, now ends it cleanly. Previously, `from_mysql` crashed Tenzir with an internal error, and `from_microsoft_sql` kept polling until the pipeline reported that it was aborted.
