---
title: Faster evaluation of logical and conditional expressions
type: change
author: jachris
pr: 5954
created: 2026-03-30T13:50:33.440315Z
---

Pipelines that use `and`, `or`, or `if`-`else` expressions run significantly faster in certain cases — up to **30×** in our benchmarks. The improvement is most noticeable in pipelines with complex filtering or branching logic. No pipeline changes are needed to benefit.
