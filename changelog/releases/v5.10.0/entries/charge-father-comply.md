---
title: "Better query optimization"
type: change
author: jachris
created: 2025-07-21T16:33:34Z
pr: 5362
---

Previously, queries that used `export` followed by a `where` that used fields
such as `this["field name"]` were not optimized. Now, the same optimizations
apply as with normal fields, improving the performance of such queries.
