# taste

Limits the input to the first *N* events per unique schema.

## Synopsis

```
taste [<limit>]
```

## Description

The `taste` operator provides an exemplary overview of the "shape" of the data
described by the pipeline. This helps to understand the diversity of the
result, especially when interactively exploring data.

### `<limit>`

An unsigned integer denoting how many events to keep per schema.

Defaults to 10.

## Examples

Get the first 10 results of each unique schema:

```
taste
```

Get the one sample for every unique event type:

```
taste 1
```
