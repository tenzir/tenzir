# taste

Limits the input to `N` events per unique schema.

```
taste [N:uint]
```

## Description

Forwards the first `N` events per unique schema and discards the rest.

The `taste` operator provides an exemplary overview of the "shape" of the data
described by the pipeline. This helps to understand the diversity of the
result, especially when interactively exploring data. 

### `N`

An unsigned integer denoting how many events to keep per schema.

Defaults to 10.

## Examples

Get 10 results of each unique schema:

```
<event stream> | taste
```

Get one sample for every unique event type:

```
<event stream> | taste 1
```
