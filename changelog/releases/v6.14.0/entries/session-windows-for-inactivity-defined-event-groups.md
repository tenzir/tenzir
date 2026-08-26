---
title: Session windows for inactivity-defined event groups
type: feature
authors:
  - mavam
created: 2026-08-24T11:15:43.131704Z
---

The `window` operator can now group events into sessions that close after a
configured period of inactivity. Consecutive events remain in the same session
while their time difference does not exceed `gap`, even when the total session
duration grows beyond that gap:

```tql
group host {
  window gap=5min, on=time, tolerance=30s {
    summarize events=count()
  }
}
```

Omit `on` for processing-time sessions. Event-time sessions accept bounded
out-of-order input through `tolerance` and can use `idle_timeout` to close after
wall-clock inactivity. An optional `size` caps a session by either duration or
event count:

```tql
window gap=5min, size=24h, on=time { … }
window gap=5min, size=1000, on=time { … }
```

A session accepts one of these caps at a time.

The operator delivers a session's events to its subpipeline in batches and
emits subpipeline output in session order. A later session cannot overtake an
earlier one when their subpipelines finish concurrently. A subpipeline that
stops early, such as one starting with `head`, consumes the remaining events of
its session; a later event within the gap then opens a new session. Checkpointing
closes the open session, so a pipeline restored from a checkpoint starts
sessions fresh.
