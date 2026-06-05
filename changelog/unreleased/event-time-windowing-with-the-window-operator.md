---
title: Event-time windowing with the window operator
type: feature
authors:
  - jachris
prs:
  - 6253
created: 2026-06-04T11:33:59.278712Z
---

The new `window` operator groups streaming events into event-time windows and
runs a subpipeline for each window:

```tql
window size=10min, every=1min, on=ts {
  summarize failures=count()
  start = $window.start
  end = $window.end
}
```

Unlike `every`, which reruns a subpipeline on a wall-clock schedule, `window`
operates on **event time**: it assigns each event to windows by the timestamp
that `on` evaluates to, and its internal clock is driven entirely by the
timestamps of the incoming events. Windows are aligned to the Unix epoch and are
either tumbling (omit `every`) or overlapping/hopping (`every < size`).

`tolerance` sets how much out-of-order lag the clock waits for before a window
closes; events that arrive after their window closed are dropped with a warning.

`idle_timeout` makes `window` also well suited to low-volume streams: a window
is emitted once it has been inactive for the given duration, so results arrive
promptly even when the next event is far off, instead of waiting for it or for
the end of the input.

Each window's `$window.start` and `$window.end` are available inside the
subpipeline, which may also end in a sink.
