---
title: "Fix hang in `cache` when creating an empty cache"
type: bugfix
author: dominiklohmann
created: 2025-03-07T18:44:22Z
pr: 5042
---

The `cache` operator no longer hangs indefinitely when creating a new cache from
a pipeline that returned zero events. For example, the pipeline `from {} | head
0 | cache "whoops"` never exited before this fix.
