---
sidebar_custom_props:
  operator:
    transformation: true
---

# tail

Limits the input to the last _N_ events.

## Synopsis

```
tail [<limit>]
```

## Description

The semantics of the `tail` operator are the same of the equivalent Unix tool:
consume all input and only display the last _N_ events.

`tail <limit>` is a shorthand notation for [`slice -<limit>:`](slice.md).

### `<limit>`

An unsigned integer denoting how many events to keep. Defaults to 10.

Defaults to 10.

## Examples

Get the last ten results:

```
tail
```

Get the last five results:

```
tail 5
```
