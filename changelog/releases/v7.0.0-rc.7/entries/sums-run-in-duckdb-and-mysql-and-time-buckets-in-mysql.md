---
title: Sums run in DuckDB and MySQL, and time buckets in MySQL
type: change
authors:
  - mavam
created: 2026-11-17T18:07:33.000000Z
---

`from_duckdb` and `from_mysql` now compute `sum` in the database, and `from_mysql` also computes time buckets with `floor`:

```tql
from_mysql table="connections", host="localhost", database="net"
hour = floor(ts, 1h)
summarize hour, bytes=sum(bytes)
```

Both databases widen a sum of integers beyond 64 bits, as a `HUGEINT` in DuckDB and a `DECIMAL` in MySQL. The sum still arrives as an `int`, and like in TQL, a sum that exceeds 64 bits becomes `null` with an "integer overflow" warning. A sum of floating-point numbers stays a `float`.

MySQL buckets `DATETIME` and `DATE` values by multiples of the width since the epoch, as TQL does, also before 1970, and the buckets arrive as times. Like comparisons with time literals, the buckets treat the values as UTC. Widths below a microsecond and multiples of a week or a month stay in the pipeline.
