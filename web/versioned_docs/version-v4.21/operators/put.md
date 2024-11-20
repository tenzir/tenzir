---
sidebar_custom_props:
  operator:
    transformation: true
---

# put

Returns new events that only contain a set of specified fields.

## Synopsis

```
put <field[=operand]>...
```

## Description

The `put` operator produces new events according to a specified list of fields.
All other fields are removed from the input.

The difference between `put` and [`extend`](extend.md) is that `put` drops all
fields not explicitly specified, whereas `extend` only appends fields.

### `<field[=operand]>`

The `field` describes the name of the field to select. The extended form with an
`operand` assignment allows for computing functions over existing fields.

If the right-hand side of the assignment
is omitted, the field name is implicitly used as an extractor. If multiple
fields match the extractor, the first matching field is used in the output. If
no fields match, `null` is assigned instead.

### Examples

Overwrite values of the field `payload` with a fixed value:

```c
put payload="REDACTED"
```

Create connection 4-tuples:

```c
put src_ip, src_port, dst_ip, dst_port
```

Unlike [`select`](select.md), `put` reorders fields. If the specified fields
do not exist in the input, `null` values will be assigned.

You can also reference existing fields:

```c
put src_ip, src_port, dst_ip=dest_ip, dst_port=dest_port
```
