---
title: "Support multiple publishers"
type: feature
author: dominiklohmann
created: 2024-06-04T12:25:36Z
pr: 4270
---

The `publish` operator's topics no longer have to be unique. Instead, any number
of pipelines may use the `publish` operator with the same topic. This enables
multi-producer, multi-consumer (MPMC) event routing, where streams of events
from different pipelines can now be merged back together in addition to being
split.

Inter-pipeline data transfer with the `publish` and `subscribe` operators is now
as fast as intra-pipeline data transfer between pipeline operators and utilizes
the same amount of memory.

Back pressure now propagates from subscribers back to publishers, i.e., if a
pipeline with a `subscribe` operator is too slow then all pipelines with
matching `publish` operators will be slowed down to a matching speed. This
limits the memory usage of `publish` operators and prevents data loss.
