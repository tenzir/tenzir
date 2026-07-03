---
title: Upper bounds for `assert_throughput`
type: feature
authors:
  - raxyte
prs:
  - 6399
created: 2026-07-01T08:45:00.000000Z
---

The `assert_throughput` operator now accepts an optional `max_events` argument
to warn when a pipeline exceeds an expected throughput range.
