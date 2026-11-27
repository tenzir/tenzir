---
title: Faster from_mysql on large result sets
type: bugfix
authors:
  - jachris
created: 2026-11-27T14:23:47.124058Z
---

The `from_mysql` operator now reads query results in larger blocks instead of making two network reads per row. This makes reading large result sets faster and reduces the load on network threads that other pipelines share.
