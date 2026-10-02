---
title: Bounded memory for limited sorting
type: change
authors:
  - mavam
created: 2026-10-01T15:15:51.994413Z
---

Sorting followed by `head` now uses memory proportional to the requested result size instead of the full input. This makes selecting a few results from a large input faster and more memory-efficient.

Get the 10 most recent events:

```tql
sort -timestamp
head 10
```

The same improvement applies to the final sort in `top` and `rare`. Find the 10 most common source IPs:

```tql
top src_ip
head 10
```

Or find the 10 least common source IPs:

```tql
rare src_ip
head 10
```

In a local benchmark, selecting 10 events from two million shuffled events was **4.2× faster** and used **65% less peak process memory** than a full sort. Small result sets benefited most; results depend on the workload and hardware.

These pipelines still inspect all input events, and `top` and `rare` still keep counts for every distinct value. Result ordering and null placement are unchanged. Without a downstream limit, `sort` still buffers the full input.
