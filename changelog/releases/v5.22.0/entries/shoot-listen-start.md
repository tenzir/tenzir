---
title: "Getting data from SentinelOne Data Lake"
type: feature
author: raxyte
created: 2025-12-12T09:10:17Z
pr: 5599
---

The new `from_sentinelone_data_lake` operator allows you to query the SentinelOne
Singularity Data Lake using PowerQuery and retrieve security events directly into
your Tenzir pipelines. Tenzir's integrations with SentinelOne now allow you to
send data to _and_ load data from SentinelOne Data Lakes.

**Example**

Query threat events and filter by severity:

```tql
from_sentinelone_data_lake "https://xdr.eu1.sentinelone.net",
  token=secret("sentinelone-token"),
  query="severity > 3 | columns id",
  start=now()-7d
```

The operator sends a request to the `/api/powerQuery` endpoint with optional time
range filters and parses the tabular response into events for downstream processing.
