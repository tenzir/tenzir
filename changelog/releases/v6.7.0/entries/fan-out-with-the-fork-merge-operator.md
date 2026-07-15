---
title: Fan out with the fork_merge operator
type: feature
authors:
  - aljazerzen
  - claude
prs:
  - 6436
created: 2026-07-10T16:16:02.073688Z
---

The new `fork_merge` operator runs multiple subpipelines on the same input
stream and merges their outputs back into a single stream. Each branch receives
a copy of every event, and the results are interleaved downstream:

```tql
subscribe "in"
fork_merge {
  summarize a=sum(bytes)
}, {
  summarize b=count()
}, {
  summarize c=max(duration)
}
publish "out"
```

Unlike `fork`, whose subpipeline must end in a sink and which forwards its
input unchanged, `fork_merge` lets you fan out independent computations and
rejoin them. Every branch is an events-to-events transformation.
