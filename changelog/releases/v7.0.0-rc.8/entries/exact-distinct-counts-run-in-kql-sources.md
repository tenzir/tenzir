---
title: Exact distinct counts run in KQL sources
type: change
authors:
  - mavam
created: 2026-11-17T16:31:42.000000Z
---

`from_microsoft_defender` and `from_azure_log_analytics` now compute `count_distinct` in the service, such as how many accounts signed in to each computer:

```tql
from_azure_log_analytics "SecurityEvent",
  workspace_id=secret("log-analytics-workspace"),
  azure_auth={
    tenant_id: "...",
    client_id: "...",
    client_secret: secret("log-analytics-client-secret"),
  }
summarize Computer, accounts=count_distinct(Account)
```

KQL computes `accounts` exactly with `count_distinctif(Account, isnotempty(Account))`, not with the approximate `dcount`. Like `count`, it leaves out empty strings, which KQL holds in place of `null`. Strings compare case-sensitively. The service fails a query that counts more than 100 million distinct values per group, as well as one that counts `dynamic` values. Use `pushdown=false` to count them in Tenzir instead.
