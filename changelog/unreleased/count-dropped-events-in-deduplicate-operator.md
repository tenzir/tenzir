---
title: Count dropped events in deduplicate operator
type: feature
authors:
  - raxyte
created: 2025-12-22T09:57:50.846474Z
---

The `deduplicate` operator now supports a `count_field` option that adds a field
to each output event showing how many events were dropped for that key.

**Example**

```tql
from {x: 1, seq: 1}, {x: 1, seq: 2}, {x: 1, seq: 3}, {x: 1, seq: 4}
deduplicate x, distance=2, count_field=drop_count
```

```tql
{x: 1, seq: 1, drop_count: 0}
{x: 1, seq: 4, drop_count: 2}
```

Events that are the first occurrence of a key or that trigger output after
expiration have a count of `0`.
