---
title: More SentinelOne filters pushed inside disjunctions
type: change
authors:
  - mavam
created: 2026-10-28T21:17:15.672695Z
---

Pipelines that filter `from_sentinelone_data_lake` with a disjunction of conjunctions now send more of that filter to SentinelOne. Previously, a single condition without a PowerQuery equivalent, such as a comparison with an IP address, kept the entire disjunction local. Now SentinelOne evaluates the rest of each branch, while Tenzir still applies the complete filter locally:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token")
where (severity == 5 and src_ip == 10.0.0.1) or severity == 6
```

Here, SentinelOne now filters for `severity == 5 or severity == 6`, where it previously received no filter at all.
