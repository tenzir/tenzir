---
title: "Add drop_nulls operator"
type: feature
authors: mavam
pr: 5370
---

The new `drop_nulls` operator removes fields containing null values from events. Without arguments, it drops all fields with null values. With field arguments, it drops only the specified fields if they contain null values.

## Examples

Drop all fields with null values:
```tql
from [{a: 1, b: null, c: 3}, {a: null, b: 2, c: null}]
drop_nulls
// [{a: 1, c: 3}, {b: 2}]
```

Drop specific fields only if they contain null:
```tql
from [{a: 1, b: null, c: 3}, {a: null, b: 2, c: null}]
drop_nulls a, b
// [{c: 3}, {b: 2, c: null}]
```

The operator maximizes performance by batching rows with identical null patterns together, avoiding unnecessary memory allocations.
