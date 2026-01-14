---
title: "Add timeout options to `summarize`"
type: feature
author: dominiklohmann
created: 2024-05-14T13:36:41Z
pr: 4209
---

The `summarize` operator gained two new options: `timeout` and `update-timeout`,
which enable streaming aggregations. They specifiy the maximum time a bucket in
the operator may exist, tracked from the arrival of the first and last event in
the bucket, respectively. The `timeout` is useful to guarantee that events are
held back no more than the specified duration, and the `update-timeout` is
useful to finish aggregations earlier in cases where events that would be sorted
into the same buckets arrive within the specified duration, allowing results to
be seen earlier.
