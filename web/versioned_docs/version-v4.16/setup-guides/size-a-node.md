---
sidebar_position: 0
---

# Size a node

To better understand what resources you need to run a node, we provide guidance
on sizing and a [calculator](#calculator) to derive concrete **CPU**, **RAM**,
and **storage** requirements.

## Considerations

Several factors have an impact on sizing. Since you can run many types of
workloads in pipelines, it is difficult to make a one-size-fits-all
recommendation. The following considerations affect your resource requirements:

### Workloads

Depending on what you do with pipelines, you may generate a different resource
profile.

#### Data Shaping

[Shaping](../user-guides/shape-data/README.md) operation changes the form of the
data, e.g., filtering events, removing columns, or changing values. This
workload predominantly incurs **CPU** load.
  
#### Aggregation

Performing in-stream or historical aggregations often requires extensive
buffering of the aggregation groups, which adds to your **RAM** requirements. If
you run intricate custom aggregation functions, you also may see an additional
increase CPU usage.

#### Enrichment

[Enriching](../user-guides/enrich-with-threat-intel/README.md) dataflows with
[contexts](../contexts.md) requires holding in-memory state proportional to the
context size. Therefore, enrichment affects your **RAM** requirements. [Bloom
filters](../contexts/bloom-filter.md) are a fixed-size space-efficient structure
for representing large sets, and [lookup tables](../contexts/lookup-table.md)
grow linearly with the number of entries.

### Data Diversity

The more data sources you have, the more pipelines you run. In the simplest
scenario where you just [import all data into a
node](../user-guides/import-into-a-node/README.md), you deploy one pipeline per
data source. The number of data sources is a thus a lower bound for the number
of pipelines.

### Data Volume

The throughput of pipeline has an impact on performance. Pipelines with low data
volume do not strain the system much, but high-volume pipelines substantially
affect **CPU** and **RAM** requirements. Therefore, understanding your ingress
volume, either as events per second or bytes per day, is helpful for sizing your
node proportionally.

### Retention

When you leverage the node's built-in storage engine by
[importing](../user-guides/import-into-a-node/README.md) and
[exporting](../user-guides/export-from-a-node/README.md) data, you need
persistent **storage**. To assess your retention span, you need to understand
your data volume and your capacity.

Tenzir storage engine builds sparse indexes to accelerate historical queries.
Based on how aggressively configure indexing, your **RAM** requirements may
vary.

## Calculator

<iframe
  src="https://tenzir-node-sizing.streamlit.app/?embed=true"
  height="1300"
  style={{ width: "100%", border: "none" }}
></iframe>
