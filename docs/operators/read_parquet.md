---
title: read_parquet
category: Parsing
example: 'read_parquet'
---

Reads events from a Parquet byte stream.

```tql
read_parquet
```

## Description

Reads events from a [Parquet][parquet] byte stream.

[Apache Parquet][parquet] is a columnar storage format that a variety of data
tools support.

[parquet]: https://parquet.apache.org/

:::tip[MMAP Parsing]
When using theis with the [`load_file`](/reference/operators/load_file) operator, we
recommend passing the `mmap=true` option to `load_file` to give the parser full control
over the reads, which leads to better performance and memory usage.
:::

:::caution[Limitation]
Tenzir currently assumes that all Parquet files use metadata recognized by
Tenzir. We plan to lift this restriction in the future.
:::

## Examples

Read a Parquet file:

```tql
load_file "/tmp/data.prq", mmap=true
read_parquet
```

## See Also

[`read_feather`](/reference/operators/read_feather),
[`to_hive`](/reference/operators/to_hive),
[`write_parquet`](/reference/operators/write_parquet)
