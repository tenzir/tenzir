# taste

Limits the input to `n` events per unique schema.

```tql
taste [n:uint]
```

## Description

Forwards the first `n` events per unique schema and discards the rest.

The `taste` operator provides an exemplary overview of the "shape" of the data
described by the pipeline. This helps to understand the diversity of the
result, especially when interactively exploring data. 

### `n: uint (optional)`

An unsigned integer denoting how many events to keep per schema.

Defaults to `10`.

## Examples

Get 10 results of each unique schema:

```tql
export
taste
```

Get one sample for every unique event type:

```tql
export 
taste 1
```
