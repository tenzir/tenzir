---
title: Processing-time, count, and trailing windows
type: feature
authors:
  - mavam
created: 2026-08-11T12:07:39.671407Z
---

The `window` operator now supports processing-time, count-based, and event-anchored trailing windows in addition to fixed event-time windows.

Omit `on` for wall-clock processing-time windows, use an unsigned `size` for event-count windows, or set `trailing=true` to run a bounded trailing window. By default, every input event fires a trailing window:

```tql
window size=5min, trailing=true, on=ts {
  summarize rolling_bytes=sum(bytes)
  this = {...$window.event, rolling_bytes: rolling_bytes}
}
```

Trailing windows expose the triggering event through `$window.event`, which enables bounded `streamstats`-style enrichment with arbitrary subpipelines. Combine `trailing=true` with `every` to sample retained history at a lower count or duration cadence:

```tql
window size=10_000, every=100, trailing=true {
  summarize p99=quantile(latency, 0.99)
  this = {...$window.event, p99: p99}
}
```

The optional `trigger` expression selects which events fire a trailing window. Every event still enters the retained history, but only events for which `trigger` evaluates to `true` run the subpipeline. This avoids replaying history for events you do not care about:

```tql
window size=10min, trailing=true, on=ts, trigger=status_id == 1 {
  summarize failures=count_if(status_id, x => x == 2)
  where failures >= 5
  this = {...$window.event, prior_failures: failures}
}
```

For slightly out-of-order event-time streams, set `tolerance` to the
maximum expected lateness. The operator reorders events within this bound
before evaluating each trailing window and reports later events through the
existing late-event warning:

```tql
window size=10min, trailing=true, on=ts, tolerance=30s {
  summarize failures=count_if(status_id, x => x == 2)
}
```

The reorder buffer can retain up to `tolerance` worth of events in addition to
the trailing `size`.
