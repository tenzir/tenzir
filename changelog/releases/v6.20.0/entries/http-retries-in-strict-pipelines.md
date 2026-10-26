---
title: HTTP retries in strict pipelines
type: bugfix
authors:
  - mavam
created: 2026-10-20T17:29:55.871043Z
---

The `from_http` operator now retries transient HTTP and connection failures inside `strict` pipelines without emitting intermediate warnings:

```tql
strict {
  from_http "https://api.example.com/events"
}
```

Previously, the first retry warning became an error and stopped the pipeline before recovery could succeed. Exhausted retries still fail the request unless `error_field` captures the HTTP error response.
