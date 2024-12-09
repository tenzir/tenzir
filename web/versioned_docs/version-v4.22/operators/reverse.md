---
sidebar_custom_props:
  operator:
    transformation: true
---

# reverse

Reverses the event order.

## Synopsis

```
reverse
```

## Description

The semantics of the `reverse` operator are the same of the Unix tool `rev`:
It reverses the order of events.

`reverse` is a shorthand notation for [`slice ::-1`](slice.md).

## Examples

Reverse a stream of events:

```
reverse
```
