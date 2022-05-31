# Parquet Plugin for VAST

The Parquet plugin enables VAST to store its data in
[Apache Parquet](https://parquet.apache.org/) format.
Parquet is a columnar file format that is optimized for efficient data storage
and retrieval, and is widely used in the data science and engineering
communities.

## Data Layout

The top-level schema stored in the parquet files is a _message envelope_, which
currently contains two columns:

| Column Name   | Column Type   | Description       |
| ------------: | ------------: | :---------------- |
| import_time   | timestamp[ns] | event import time |
| event         | struct        | actual event data |

VAST is using Apache Arrow under the hood, and the Arrow schema is stored
in serialized form as part of the parquet metdata under the key `ARROW:schema`.
The Arrow documentation section about [parquet](https://arrow.apache.org/docs/dev/cpp/parquet.html)
discusses the mapping of types between Arrow and Parquet.

## Limitations / Differences to existing segment store plugin

### Table slice granularity

Apache parquet implementation produces one record batch of data per
[row group](https://parquet.apache.org/docs/concepts/), which is converted into
a _table slice_, the smallest unit of data used in VAST internally. For storage
efficiency, row groups are typically larger than table slices created during
import, which are smaller to increase data recency and immediate availability.
The `import_time`, stored and associated per table slice, is not correctly
maintained in the larger table slice that spans multiple initial slices.
VAST uses the largest of the available import times.
Note that the information is properly stored for each event inside the parquet
file, and we plan to fix that deficiency in an upcoming release.

### Performance considerations

The current implementation does not optimize query execution for the underlying
columnar storage, in particular it doesn't leverage predicate pushdown and
projection at the partition or file level.

1. Row-based candidate checks don't utilize the columnar format to its fullest.
   - Evaluates candidate events row by row.
   - Should evaluate one column at a time, using e.g. Arrow compute functions.
2. Queries read the entire table, not just columns relevant for query execution.
   - For point queries with a low matching probability, reading only the rows
     utilized in the query expression can reduce I/O, memory and CPU usage.
