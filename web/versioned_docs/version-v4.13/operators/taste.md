---
sidebar_custom_props:
  operator:
    transformation: true
---

# taste

Limits the input to *N* events per unique schema.

## Synopsis

```
taste [<limit>]
```

## Description

The `taste` operator provides an exemplary overview of the "shape" of the data
described by the pipeline. This helps to understand the diversity of the
result, especially when interactively exploring data. Usually, the first *N*
events are returned, but this is not guaranteed.

### `<limit>`

An unsigned integer denoting how many events to keep per schema.

Defaults to 10.

## Examples

Get 10 results of each unique schema:

```
taste
```

Get one sample for every unique event type:

```
taste 1
```
