Unary operators like `move` or `not` may now be used like function calls. For
example, instead of using `(move x).frobnify()`, TQL now also supports
`move(x).frobnify()` or `x.move().frobnify()`.
