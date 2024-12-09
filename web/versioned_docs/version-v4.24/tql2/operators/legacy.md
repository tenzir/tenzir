# legacy

:::warning
This operator will be removed once TQL2 is stable and all TQL1 functionality is
ported over.
:::

Provides a compatibility fallback to TQL1 pipelines.

```tql
legacy definition:string
```

## Description

This operator takes a TQL1 pipeline as a string and evaluates it.

### `definition: string`

The TQL1 pipeline to evaluate.

## Examples

### Use the legacy chart operator

```tql
summarize ts, count()
legacy "chart area"
```
