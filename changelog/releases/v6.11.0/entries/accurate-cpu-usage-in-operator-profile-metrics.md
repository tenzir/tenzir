---
title: Accurate CPU usage in operator profile metrics
type: bugfix
authors:
  - aljazerzen
  - claude
created: 2026-08-11T18:13:07.667146Z
---

The `cpu` field of the `operator_profile` metrics no longer reports inflated or
negative values, and every event now carries the time span its counters cover.

`cpu` is the share of a CPU core an operator used since the previous sample. It
was computed by dividing the consumed CPU time by the nominal sampling interval
of one second rather than by the time that actually passed. Samples are taken by
the pipeline itself, so they arrive late exactly when the pipeline is busy, and
the reported usage grew with the delay. An operator that cannot use more than
one core could report well above `100`. Operators that had already finished
could report a negative value.

Each event now also has a `duration` field holding the measured time span its
counters cover, so rates no longer have to assume that events arrive exactly one
second apart:

```tql
metrics "operator_profile"
events_per_second = events_out / duration.count_seconds()
```

The window of the first sample of a pipeline now starts when the pipeline began
executing. It previously started when the metrics collection happened to be
scheduled, which inflated the first reported rates by however long that took.
