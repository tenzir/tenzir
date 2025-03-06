# ClickHouse

[clickhouse]: https://clickhouse.com/
[clickhouse-cpp]: https://github.com/ClickHouse/clickhouse-cpp/
[operator-docs]: ../../tql2/operators/to_clickhouse.md

[ClickHouse][clickhouse] is a high-performance, column-oriented SQL
database management system (DBMS) for online analytical processing (OLAP).
It is available as both an open-source software and a cloud offering.

![ClickHouse](clickhouse.svg)

Tenzir sends data to ClickHouse using the open-source [clickhouse-cpp][clickhouse-cpp]
library, enabling efficient columnar transfer of events.

The [`to_clickhouse`][operator-docs] operator can both write to existing tables and create
new tables.

:::info Advanced Details
For details, refer to the documentation for the [`to_clickhouse`][operator-docs].
operator.
:::

## Examples

```tql title="Send a CSV file to a ClickHouse table"
from "my_file.csv"
to_clickhouse table="my_table"
```
