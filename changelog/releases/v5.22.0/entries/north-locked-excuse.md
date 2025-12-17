---
title: "Simplified `publish` and `subscribe` connection"
type: change
author: IyeOnline
created: 2025-12-16T15:04:16Z
pr: 5597
---

We made an under-the-hood change to the `publish` and `subscribe` implementation
that reduces the overhead when publishing to high-throughput topics.
