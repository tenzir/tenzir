# replace

Replaces the fields matching the given extractors with fixed values.

## Synopsis

```
replace <field=operand>...
```

## Description

The `replace` operator mutates existing fields by providing a new value.

The difference between `replace` and [`extend`](extend.md) is that `replace`
overwrites existing fields, whereas `extend` doesn't touch the input.

### `<field=operand>`

The assignment consists of `field` that describes an existing field name and
`operand` that defines the new field value.

If `field` does not exist in the input, the operator degenerates to
[`pass`](pass.md).

### Examples

Replace the field the field `src_ip` with a fixed value:

```
replace src_ip=0.0.0.0
```
