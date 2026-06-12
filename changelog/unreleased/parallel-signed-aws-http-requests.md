---
title: Parallel signed AWS HTTP requests
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6188
created: 2026-06-12T06:12:17.652016Z
---

AWS-backed operators that issue parallel signed HTTP requests now send concurrent requests over separate pooled connections.

Previously, HTTP/1.1 request pipelining could serialize parallel requests behind a single connection, so `parallel` settings for sinks such as `to_amazon_cloudwatch` did not provide the intended concurrency.
