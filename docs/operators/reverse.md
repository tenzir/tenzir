---
title: reverse
category: Analyze
example: 'reverse'
---
Reverses the event order.

```tql
reverse
```

## Description

`reverse` is a shorthand notation for [`slice stride=-1`](/reference/operators/slice).

:::note[Potentially High Memory Usage]
Use caution when applying this operator to large inputs. It currently buffers
all data in memory. Out-of-core processing is on our roadmap.
:::

## Examples

### Reverse a stream of events

```tql
from {x: 1}, {x: 2}, {x: 3}
reverse
```

```tql
{x: 3}
{x: 2}
{x: 1}
```

## See Also

[`sort`](/reference/operators/sort)
