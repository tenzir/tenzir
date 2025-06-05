---
title: assert
category: Filter
example: 'assert name.starts_with("John")'
---

Drops events and emits a warning if the invariant is violated.

```tql
assert invariant:bool
```

## Description

The `assert` operator asserts that `invariant` is `true` for events. In case an
event does not satisfy the invariant, it is dropped and a warning is emitted.
The only difference between `assert` and `where` is that latter does not emit
such a warning.

## Examples

### Make sure that all events satisfy `x > 2`

```tql
from {x: 1}, {x: 2}, {x: 1}
assert x > 2
```

```tql
{x: 1}
// warning: assertion failure
{x: 1}
```

### Check that a topic only contains certain events

```tql
subscribe "network"
assert @name == "ocsf.network_activity"
// continue processing
```

## See Also

[`assert_throughput`](/reference/operators/assert_throughput),
[`where`](/reference/operators/where)
