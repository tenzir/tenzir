---
title: Retry temporary HTTP connection failures
type: bugfix
authors:
  - raxyte
created: 2026-08-20T00:00:00.000000Z
---

HTTP-based pipelines with retries configured no longer fail immediately when a
connection is temporarily unavailable. This includes SQS, Splunk exports, and
CloudWatch Live Tail.
