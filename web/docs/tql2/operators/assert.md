# assert

Asserts that an invariant is held or that a condition is satisfied.

```tql
assert invariant:expr
```

## Description

The `assert` operator _asserts_ that `invariant` is `true` for all evaluations
of the expression.

Use `assert` to ensure the shape or other features of the data meet your
assumptions. Tenzir's expression language offers various ways to describe the desired data. In particular,
expressions work *across schemas* and thus make it easy to concisely articulate constraints.

### `invariant`

The `predicate` is an [expression](../language/expressions.md) that is evaluated and tested for each event.
The evaluation must result in a boolean value.

If the result is `true`, execution continues as normal. <br/>
If the result is `false`, the operator emits a diagnostic and stops the
execution immediately.
XXX: Is the above correct?

## Examples

// XXX: Figure examples
