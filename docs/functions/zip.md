---
title: zip
category: List
example: 'zip(xs, ys)'
---
Combines two lists into a list of pairs.

```tql
zip(xs:list, ys:list) -> list
```

## Description

The `zip` function returns a list containing records with two fields `left` and
`right`, each containing the respective elements of the input lists.

If both lists are null, `zip` returns null. If one of the lists is null or has a
mismatching length, missing values are filled in with nulls, using the longer
list's length, and a warning is emitted.

## Examples

### Combine two lists

```tql
from {xs: [1, 2], ys: [3, 4]}
select zs = zip(xs, ys)
```

```tql
{
  zs: [
    {left: 1, right: 3},
    {left: 2, right: 4}
  ]
}
```

## See Also

[`concatenate`](/reference/functions/concatenate),
[`map`](/reference/functions/map)
