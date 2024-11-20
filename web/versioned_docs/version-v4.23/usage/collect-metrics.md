---
sidebar_position: 6
---

# Collect metrics

Tenzir keeps track of metrics about node resource usage, pipeline state, and
runtime performance.

Metrics are stored as internal events in the node's storage engine, allowing you
to work with metrics just like regular data. Use the
[`metrics`](../operators/metrics.md) source operator to access the metrics. The
operator documentation lists all available metrics in detail.

The `metrics` operator provides a *copy* of existing metrics. You can use it
multiple time to reference the same metrics feed.

## Write metrics to a file

Export metrics continuously to a file via `metrics --live`:

```yaml
metrics --live
| to /var/log/tenzir/metrics.json --append write json --compact-output
```

This attaches to incoming metrics feed and forward them as NDJSON automatically
to a file. Without `--live`, the `metrics` operator returns the snapshot of all
historical metrics.

## Summarize metrics

You can [shape](../usage/shape-data/README.md) metrics like ordinary data,
e.g., write aggregations over metrics to compute runtime statistics suitable for
reporting or dashboarding:

```
metrics
| where #schema == "tenzir.metrics.operator" && sink == true
| summarize runtime=sum(duration) by pipeline_id
| sort runtime desc
```

The above example computes the total runtime over all pipelines grouped by their
unique ID.
