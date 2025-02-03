# legacy

:::warning Migrate to TQL2
TQL1 is deprecated and will be removed with the upcoming **Tenzir Node v5.0**
release, which will make TQL2 the default.

Currently, TQL2 is opt-in. You probably got to this page because you've seen a
warning in your TQL1 pipelines that pointed you to this page.

To prepare for this migration and use TQL2 for all pipelines today, we recommend
setting the environment variable `TENZIR_TQL2=true`.

With **Tenzir Node v6.0**, the `legacy` operator and TQL1 will be removed
entirely.
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
