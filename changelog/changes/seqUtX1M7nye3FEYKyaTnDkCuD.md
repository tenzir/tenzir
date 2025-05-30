---
title: "Add 'min_events' parameters to /serve endpoint"
type: feature
authors: lava
pr: 3666
---

We optimized the behavior of the 'serve' operator to respond
quicker and cause less system load for pipelines that take a
long time to generate the first result. The new `min_events`
parameter can be used to implement long-polling behavior for
clients of `/serve`.
