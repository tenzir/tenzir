---
sidebar_custom_props:
  operator:
    source: true
---

# metrics

Retrieves metrics events from a Tenzir node.

## Synopsis

```
metrics [--live]
```

## Description

The `metrics` operator retrieves [metrics events](../metrics.md) from a Tenzir
node.

### `--live`

Work on all metrics events as they are generated in real-time instead of on
metrics events persisted at a Tenzir node.

## Examples

View all metrics generated in the past five minutes.

```
metrics
| where #import_time > 5 minutes ago
```

Show the total runtime of all pipelines by their ID.

```
metrics
| where #schema == "tenzir.metrics.operator" && sink == true
| summarize runtime=sum(duration) by pipeline_id
```
