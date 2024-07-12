---
sidebar_custom_props:
  operator:
    transformation: true
---

# head

Limits the input to the first _N_ events.

## Synopsis

```
head [<limit>]
```

## Description

The semantics of the `head` operator are the same of the equivalent Unix tool:
process a fixed number of events from the input. The operator terminates
after it has reached its limit.

`head <limit>` is a shorthand notation for [`slice :<limit>`](slice.md).

### `<limit>`

An unsigned integer denoting how many events to keep.

Defaults to 10.

## Examples

Get the first ten events:

```
head
```

Get the first five events:

```
head 5
```
