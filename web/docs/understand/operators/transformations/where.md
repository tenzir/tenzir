# where

Filters events according to an [expression](../../expressions.md).

## Synopsis

```
where <expression>
```

## Description

The `where` operator only keeps events that match the provided
[expression](../../expressions.md) and discards all other events.

Use `where` to extract the subset of interest of the data. VAST's expression
language offers various ways to describe the desired data. In particular,
expressions work *across schemas* and thus make it easy to concisely articulate
constraints.

### `<expression>`

The [expression](../../expressions.md) to evaluate for each event.

## Examples

Select all events that contain a field with the value `1.2.3.4`:

```
where 1.2.3.4
```

This expression internally completes to `:ip == 1.2.3.4`. The type extractor
`:ip` describes all fields of type `ip`. Use field extractors to only consider a
single field:

```
where src_ip == 1.2.3.4
```

As a slight variation of the above: use a nested field name and a temporal
constraint of the field with name `ts`:

```
where id.orig_h == 1.2.3.4 && ts > 1 hour ago
```

Subnets are first-class values:

```
where 10.10.5.0/25
```

This expression unfolds to `:ip in 10.10.5.0/25 || :subnet == 10.10.5.0/25`. It
means "select all events that contain a field of type `ip` in the subnet
`10.10.5.0/25`, or a field of type `subnet` the exactly matches `10.10.5.0/25`".

Expressions consist of predicates that can be connected with AND, OR, and NOT:

```
where 10.10.5.0/25 && (orig_bytes > 1 Mi || duration > 30 min)
```
