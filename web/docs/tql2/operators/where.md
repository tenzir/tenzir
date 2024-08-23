# where

Filters events according to an [expression](../language/expressions.md).

```tql
where predicate:bool
```

## Description

The `where` operator only keeps events that match the provided
[predicate](../language/expressions.md) and discards all other events.

Use `where` to extract the subset of interest of the data. Tenzir's expression
language offers various ways to describe the desired data. In particular,
expressions work *across schemas* and thus make it easy to concisely articulate
constraints.

### `predicate: bool`

The `predicate` is an [expression](../language/expressions.md) that is evaluated and tested for each event.

## Examples

Keep only events where `src_ip` is `1.2.3.4`.

```tql
where src_ip == 1.2.3.4
```

Use a nested field name and a temporal constraint of the field with name `ts`:

```tql
where id.orig_h == 1.2.3.4 and ts > now() - 1h
```

Expressions consist of predicates that can be connected with `and`, `or`, and
`not`:

```tql
where net == 10.10.5.0/25 and (orig_bytes > 1Mi or duration > 30min)
```
