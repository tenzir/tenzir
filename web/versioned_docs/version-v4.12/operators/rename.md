---
sidebar_custom_props:
  operator:
    transformation: true
---

# rename

Renames fields and types.

## Synopsis

```
rename <name=extractor>...
```

## Description

The `rename` operator assigns new names to fields or types. Renaming only
modifies metadata and is therefore computationally inexpensive. The operator
handles nested field extractors as well, but cannot perform field reordering,
e.g., by hoisting nested fields into the top level.

Renaming only takes place if the provided extractor on the right-hand side of
the assignment resolves to a field or type. Otherwise the assignment does
nothing. If no extractors match, `rename` degenerates to [`pass`](pass.md).

### `<name=extractor>...`

An assignment of the form `name=extractor` renames the field or type identified
by `extractor` to `name`.

## Examples

Rename events of type `suricata.flow` to `connection`:

```
rename connection=:suricata.flow
```

Assign new names to the fields `src_ip` and `dest_ip`:

```
rename src=src_ip, dst=dest_ip
```

Give the nested field `orig_h` nested under the record `id` the name `src_ip`:

```
rename src=id.orig_h
```

Same as above, but consider fields at any nesting hierarchy:

```
rename src=orig_h
```
