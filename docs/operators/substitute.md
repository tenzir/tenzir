---
title: substitute
category: Transform
example: 'substitute what=42, with=null'
---

Replaces all occurrences of a value with another value.

```tql
substitute [path:field], what=any with=any
```

## Description

The `substitute` operator scans all fields of each input event (optionally
limited to a specific field) and replaces every occurrence of a value equal
to `what` with the value specified by `with`. This is useful for normalizing,
redacting, or cleaning up data in bulk.

- If `with` is `null`, matching values are replaced with `null`.
- If `path` is omitted, substitution is performed recursively on all fields of the event.
- The types of `what` and `with` must match, unless one of them is `null`.

### `path: field (optional)`

The field path to restrict substitution to. If omitted, all fields are considered.

### `what: any`

The value to search for and replace.

### `with: any`

The value to substitute in place of `what`.

## Examples

### Replace all occurrences of 42 with null

```tql
substitute what=42, with=null
```

### Replace all empty strings with "N/A" in the field `user.name`

```tql
substitute user.name, what="" with="N/A"
```

### Replace a specific IP address with a redacted value

```tql
substitute what=192.168.1.1, with="REDACTED"
```

### Replace missing values in a field with a default

```tql
substitute status, what=null, with="unknown"
```

## See Also

[`replace`](/reference/operators/replace), [`drop`](/reference/operators/drop), [`map`](/reference/operators/map)
