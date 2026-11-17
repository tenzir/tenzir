---
title: Pipe sources compute the values that a summary reads
type: change
authors:
  - mavam
created: 2026-11-15T19:20:32.505768Z
---

`from_splunk`, `from_microsoft_defender`, and `from_azure_log_analytics` now also take over a `summarize` that groups by or aggregates computed values, such as values that a `set` before it assigns:

```tql
from_splunk "https://splunk:8089", index="main", earliest="-1h", latest="now"
kind = action.to_upper()
summarize kind, events=count()
```

Splunk computes the key with `| eval tenzir_column_0=upper('action')` before its `stats`, and KQL with `| extend` before its `summarize`.
