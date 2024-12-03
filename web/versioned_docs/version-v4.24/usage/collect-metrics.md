---
sidebar_position: 6
---

# Collect metrics

Tenzir keeps track of metrics about node resource usage, pipeline state, and
runtime performance.

Metrics are stored as internal events in the node's storage engine, allowing you
to work with metrics just like regular data. Use the
[`metrics`](../tql2/operators/metrics.md) source operator to access the metrics.
The operator documentation lists [all available
metrics](../tql2/operators/metrics#schemas) in detail.

The `metrics` operator provides a *copy* of existing metrics. You can use it
multiple time to reference the same metrics feed.

## Write metrics to a file

Export metrics continuously to a file via `metrics --live`:

```tql
metrics live=true
write_ndjson
save_file "metrics.json", append=true
```

This attaches to incoming metrics feed, renders them as NDJSON, and then writes
the output to a file. Without the `live` option, the `metrics` operator returns
the snapshot of all historical metrics.

## Summarize metrics

You can [shape](../usage/shape-data/README.md) metrics like ordinary data,
e.g., write aggregations over metrics to compute runtime statistics suitable for
reporting or dashboarding:

```tql
metrics
where @name == "tenzir.metrics.operator" and sink == true
summarize runtime=sum(duration), pipeline_id
sort -runtime
```

The above example computes the total runtime over all pipelines grouped by their
unique ID.
