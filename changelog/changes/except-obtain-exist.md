---
title: "Add frequency based emission to summarize"
type: feature
authors: tobim
pr: 5605
---

The `summarize` operator now supports periodic emission of aggregation results at
fixed intervals, enabling real-time streaming analytics and monitoring use cases.

Use the optional `frequency` parameter to emit results every N seconds:

```tql
summarize {frequency: 5s}, count(this), src_ip
```

This emits aggregation results every 5 seconds, showing the count per source IP for
events received during each interval:

```tql
{src_ip: 192.168.1.1, count: 42}
{src_ip: 192.168.1.2, count: 17}
// ... 5 seconds later ...
{src_ip: 192.168.1.1, count: 38}
{src_ip: 192.168.1.3, count: 9}
```

The `mode` parameter controls how aggregations behave across emissions:

**Reset mode** (default) resets aggregations after each emission, providing
per-interval metrics:

```tql
summarize {frequency: 10s}, sum(bytes)
// Shows bytes per 10-second window
```

**Cumulative mode** accumulates values across emissions, providing running totals:

```tql
summarize {frequency: 10s, mode: "cumulative"}, sum(bytes)
// Shows total bytes seen so far
```

**Update mode** only emits when values change from the previous emission, reducing
output noise in monitoring scenarios:

```tql
summarize {frequency: 1s, mode: "update"}, count(this), severity
// Only emits when the count for a severity level changes
```

The operator always emits final results when the input stream ends, ensuring no
data is lost.
