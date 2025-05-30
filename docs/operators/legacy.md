---
title: legacy
---

Provides a compatibility fallback to TQL1 pipelines.

```tql
legacy definition:string
```

:::warning[Migrate to TQL2]
The `legacy` operator is deprecated and will be removed in a future release.
TQL1 features will be removed as-needed without further notice.
:::

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
