---
title: Parse ingest endpoint bodies with a subpipeline
type: feature
authors:
  - lava
created: 2026-11-02T17:19:23.884173Z
---

The `ingest` operator now takes an optional parser pipeline that turns the body of each request into events, like `accept_http` does:

```tql
ingest "okta-webhooks" {
  read_ndjson
}
```

Inside the pipeline, `$request` holds the request's `received_at`, `method`, `path`, `query`, and `headers`. A request that fails to parse emits a warning and does not stop the pipeline.
