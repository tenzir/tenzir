---
title: More aggregation functions run in the source
type: change
authors:
  - mavam
created: 2026-11-15T18:57:44.724135Z
---

Sources now hand their targets more aggregation functions of a `summarize`: `count_if`, `count_distinct`, `sum`, and `mean`, where the target has an exact equivalent. For example:

```tql
from_microsoft_sql table="events", host="db", user="u", password=secret("p"), database="d"
summarize big=count_if(n, v => v > 0), kinds=count_distinct(n), total=sum(id), avg=mean(id)
```

SQL Server computes this as `COUNT_BIG(CASE WHEN (([n] IS NOT NULL) AND ([n] > 0)) THEN [n] END)`, `COUNT_BIG(DISTINCT [n])`, `SUM([id])`, and `AVG(CAST([id] AS float))`. DuckDB and MySQL compute all but `sum`, ClickHouse and Hydrolix all four, Splunk all four with `count(eval(...))`, `dc`, `sum`, and `avg`, and Microsoft Defender and Azure Log Analytics `count_if` with `countif`. SentinelOne computes `count_distinct` with the approximate `estimate_distinct`, since PowerQuery has no exact distinct count.
