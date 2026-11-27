---
title: Read and write Axiom datasets with APL pushdown
type: feature
authors:
  - mavam
created: 2026-11-22T08:17:53.996523Z
---

The new `from_axiom` and `to_axiom` operators query and ingest events through Axiom's regional edge APIs.

`to_axiom` sends gzip-compressed NDJSON batches to an Axiom dataset and reports rejected events and ingest warnings returned in HTTP 200 responses.

`from_axiom` reads from a dataset or a raw APL query and pushes downstream `where`, `select`, `drop`, `summarize`, `sort`, and `head` stages into Axiom Processing Language (APL). For example:

```tql
from_axiom "http-logs", token=secret("axiom_token")
where status >= 400 and uri.starts_with("/api/")
bucket = _time.floor(5min)
summarize bucket, method, errors = count(), unique_ips = count_distinct(src_ip)
sort -errors
head 10
```

This runs directly in Axiom as:

```kusto
['http-logs']
| where (['status'] >= 400)
| where (['uri'] startswith_cs "/api/")
| extend ['__tenzir_column_0'] = bin(['_time'], timespan(300000000000ns))
| summarize ['__tenzir_part_0'] = count() by ['__tenzir_column_0'], ['method'], ['src_ip']
| summarize ['__tenzir_column_1'] = sum(['__tenzir_part_0']), ['__tenzir_column_2'] = countif(isnotnull(['src_ip'])) by ['__tenzir_column_0'], ['method']
| sort by ['__tenzir_column_1'] desc
| take 10
```

Pushed stages run with APL's native semantics; pass `pushdown=false` to evaluate them in Tenzir instead.
