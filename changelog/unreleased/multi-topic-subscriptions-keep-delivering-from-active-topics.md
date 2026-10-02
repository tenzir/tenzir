---
title: Multi-topic subscriptions keep delivering from active topics
type: bugfix
authors:
  - aljazerzen
created: 2026-10-01T10:16:14.652637Z
---

Multi-topic subscriptions continue receiving events from active topics even when another topic has no publisher. For example, `subscribe "alerts", "idle" | head 3` can finish after three events from `alerts` without waiting for `idle`.

Filters still run before the limit, which applies to the combined output of all subscribed topics.
