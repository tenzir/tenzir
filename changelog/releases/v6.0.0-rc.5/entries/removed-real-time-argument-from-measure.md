---
title: Removed `real_time` argument from `measure`
type: breaking
author: aljazerzen
pr: 5880
created: 2026-04-30T13:01:39.565524Z
---

The `measure` operator no longer accepts the `real_time` argument. The
operator's emission cadence is now governed entirely by the executor's
backpressure, so the option no longer has a meaningful effect.

Remove `real_time=true` or `real_time=false` from your pipelines:

```tql
// Before:
measure real_time=true

// After:
measure
```
