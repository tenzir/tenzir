---
title: Timeout flushing for OpenSearch and CloudWatch sinks
type: bugfix
authors:
  - mavam
  - codex
prs:
  - 6188
created: 2026-06-12T06:12:17.652562Z
---

The `to_opensearch` and `to_amazon_cloudwatch` sinks now reliably flush partial batches after their configured timeout while the pipeline continues to run.

Previously, concurrent wakeups could race with batch deadline updates, which could cause timeout-based flushes or CloudWatch send-completion handling to be missed.
