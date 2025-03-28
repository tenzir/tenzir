# legacy

Provides a compatibility fallback to TQL1 pipelines.

:::warning Subject to change
Future releases after Tenzir Node v5.0 will remove TQL1 piece-by-piece without
further notice. Please migrate away from the `legacy` as soon as you can.
:::

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
