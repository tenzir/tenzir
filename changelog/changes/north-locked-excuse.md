---
title: "Simplified `publish` and `subscribe` connection"
type: change
authors: IyeOnline
pr: 5597
---

We made an under-the-hood change to the `publish` and `subscribe` implementation
that reduces the overhead when publishing to high-throughput topics.
