---
title: Reset grouped summarize state after periodic emission
type: bugfix
authors:
  - raxyte
prs:
  - 6435
created: 2026-07-10T08:25:31+00:00
---

The `summarize` operator no longer retains inactive group keys when using
periodic emission in `reset` mode. Previously, every interval emitted one event
for every group seen since the pipeline started, including inactive groups with
reset aggregate values. This caused output batches and import metrics to grow
over time. Each interval now contains only groups that received events during
that interval.
