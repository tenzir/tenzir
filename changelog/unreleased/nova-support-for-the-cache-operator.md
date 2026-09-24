---
title: Nova support for the cache operator
type: feature
authors:
  - aljazerzen
created: 2026-09-24T14:10:41.390536Z
---

The `cache` operator now works in Nova pipelines that run in the node process, including transparent read-write replay:

```tql
from {x: 1}, {x: 2}
cache "recent-events"
```

Node pipelines can share named Nova caches while retaining capacity limits and expiration behavior. Client-side pipelines now reject `cache` because their process-local storage cannot be shared with the node.
