---
title: Event-based rate limiting for throttle operator
type: change
author: raxyte
pr: 5642
created: 2026-01-09T10:59:50.278678Z
---

The `throttle` operator now rate-limits events instead of bytes. Use the `rate`
option to specify the maximum number of events per window, `weight` to assign
custom per-event weights, and `drop` to discard excess events instead of
waiting. The operator also emits metrics for dropped events.
