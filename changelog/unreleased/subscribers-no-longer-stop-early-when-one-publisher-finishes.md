---
title: Subscribers no longer stop early when one publisher finishes
type: bugfix
authors:
  - IyeOnline
prs:
  - 6467
created: 2026-07-22T15:28:27.793555Z
---

Subscribers to internal pub/sub topics (used by the `subscribe` and `publish`
operators) no longer stop early just because one publisher for that topic
finished. Previously, a `publish` pipeline completing normally could cause
the node to signal "topic exhausted" to all subscribers of that topic, even
while the node kept running and another publisher for the same topic could
still start later. This could make a running `subscribe "topic"` pipeline
terminate prematurely. Subscribers are now only told a topic is exhausted
when the node itself is actually shutting down.
