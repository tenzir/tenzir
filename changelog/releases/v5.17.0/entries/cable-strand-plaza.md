---
title: "Memory usage when importing many different schemas at once"
type: change
author: [tobim, jachris]
created: 2025-10-10T17:22:28Z
pr: 5508
---

Previously, importing a high volume of highly heterogeneous events could lead to
memory usage issues because of internal buffering that was only limited on a
per-schema basis. With the introduction of a global limit across all schemas,
this issue has now been fixed. The configuration option
`tenzir.max-buffered-events` can be used to tune the new buffering limits.
