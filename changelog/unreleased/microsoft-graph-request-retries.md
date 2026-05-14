---
title: Microsoft Graph request retries
type: bugfix
authors:
  - mavam
  - codex
created: 2026-05-14T20:27:32.016844Z
---

The `from_microsoft_graph` operator now retries throttled and transient Microsoft Graph requests with a bounded default retry policy.

This helps Graph collection pipelines continue through `429 Too Many Requests`, `503 Service Unavailable`, and `504 Gateway Timeout` responses. When Microsoft Graph returns `Retry-After`, Tenzir waits for the requested delay before retrying.
