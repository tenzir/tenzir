# legacy

:::warning
This operator will be removed once TQL2 is stable and all TQL1 functionality is
ported over.
:::

Provides a compatibility fallback to TQL1 pipelines.

```tql
legacy definition:str
```

## Description

This operator takes a TQL1 pipeline as a string and evaluates it.

### `definition: str`

The TQL1 pipeline to evaluate.

## Examples

```tql
summarize ts, count()
legacy "chart area"
```
