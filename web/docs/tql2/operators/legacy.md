# legacy

:::warning
This operator will be removed once TQL2 is stable and all TQL1 functionality is
ported over.
:::

```tql
legacy pipeline:str
```

## Description 

This operator takes a TQL1 pipeline as a string and evaluates it.

### `pipeline: str`

The TQL1 pipeline to evaluate.

## Examples

```tql
export
summarize ts, count(.)
legacy "chart area"
```
