This release enhances the sort function with custom comparators and descending order support, and extends the slice function to work with lists.

## 🚀 Features

### Enhance `sort` function with `desc` and `cmp` parameters

The `sort` function now supports two new parameters: `desc` for controlling sort direction and `cmp` for custom comparison logic via binary lambdas.

**Sort in descending order:**

```tql
from {xs: [3, 1, 2]}
select ys = sort(xs, desc=true)
```

```tql
{ys: [3, 2, 1]}
```

**Sort records by a specific field using a custom comparator:**

```tql
from {xs: [{v: 2, id: "b"}, {v: 1, id: "a"}, {v: 2, id: "c"}]}
select ys = sort(xs, cmp=(left, right) => left.v < right.v)
```

```tql
{
  ys: [
    {v: 1, id: "a"},
    {v: 2, id: "b"},
    {v: 2, id: "c"},
  ],
}
```

The `cmp` lambda receives two elements and returns a boolean indicating whether the first element should come before the second. Both parameters can be combined to reverse a custom comparison.

*By @mavam and @codex in #5767.*

### Slice function extended to support lists

The `slice` function now supports `list` types in addition to `string`. You can slice lists using the same `begin`, `end`, and `stride` parameters. Negative stride values are now supported for lists, letting you reverse or step backward through list data. String slicing continues to require a positive `stride`.

Example usage with lists:

- `[1, 2, 3, 4, 5].slice(begin=1, end=4)` returns `[2, 3, 4]`
- `[1, 2, 3, 4, 5].slice(stride=-1)` returns the list in reverse order
- `[1, 2, 3, 4, 5].slice(begin=1, end=5, stride=-2)` returns `[5, 3]`

*By @mavam and @codex in #5819.*

## 🐞 Bug fixes

### Fix `read_lines` operator for old executor

The `read_lines` operator was accidently broken while it was ported to the new execution API. This change restores its functionality.

*By @tobim.*

### HTTP header values can contain colons

HTTP header values containing colons are now parsed correctly.

*By @lava in #5693.*
