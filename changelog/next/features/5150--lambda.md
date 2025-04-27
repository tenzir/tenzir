TQL now supports lambda expressions. They are supported in the `where` and `map`
functions on list, and on the newly added `count_where` aggregation function.
Instead of `[1, 2, 3].map(x, x + 1)`, use `[1, 2, 3].map(x => x + 1)`. This
subtle change makes it obvious that the expression is not evaluated on the
entire list, but rather on each element individually.

The `count_where` aggregation function counts the number of elements in a list
that satisfy a given predicate. For example, `[1, 2, 3].count_where(x => x > 1)`
returns `2`.
