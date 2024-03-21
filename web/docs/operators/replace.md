---
sidebar_custom_props:
  operator:
    transformation: true
---

# replace

Replaces the fields matching the given extractors with fixed values.

## Synopsis

```
replace <extractor=operand>...
```

## Description

The `replace` operator mutates existing fields by providing a new value.

The difference between `replace` and [`extend`](extend.md) is that `replace`
overwrites existing fields, whereas `extend` doesn't touch the input.

### `<extractor=operand>`

The assignment consists of an `extractor` that matches against existing fields
and an `operand` that defines the new field value.

If `field` does not exist in the input, the operator degenerates to
[`pass`](pass.md). Use the [`set`](set.md) operator to extend fields that cannot
be replaced.

### Examples

Replace the field the field `src_ip` with a fixed value:

```
replace src_ip=0.0.0.0
```

Replace all IP address with a fixed value:

```
replace :ip=0.0.0.0
```
