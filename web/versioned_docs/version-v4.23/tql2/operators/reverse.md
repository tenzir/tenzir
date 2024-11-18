# reverse

Reverses the event order.

```tql
reverse
```

## Description

`reverse` is a shorthand notation for [`slice stride=-1`](slice.md).

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

## Examples

### Reverse a stream of events

```tql
from [{x: 1}, {x: 2}, {x: 3}]
reverse
```

```tql
{x: 3}
{x: 2}
{x: 1}
```
