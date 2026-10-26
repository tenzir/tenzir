---
title: SentinelOne queries through the console API
type: change
authors:
  - mavam
created: 2026-10-03T18:20:47.761563Z
---

The `from_sentinelone_data_lake` operator now uses SentinelOne's Long Running Query API instead of the V1 PowerQuery endpoint, which retires on February 15, 2027.

Replace your regional `xdr.*.sentinelone.net` URL with your tenant's console URL and replace scoped SDL Log Read keys with a console service-user API token:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-console-token"),
  query="severity > 3 | columns id | limit 5000",
  account_ids=["1234567890123456789"],
  timeout=2min
```

The optional `account_ids` list selects specific accounts instead of tenant scope. Without explicit time bounds, queries cover the past 24 hours. The new `timeout` option bounds launching and polling, including retries, to `3min` by default. On expiry, the operator attempts to cancel the query and reports an error. PowerQuery still defaults to at most 1,000 rows for queries without `limit` or `group`; include `| limit N` in the query to request more rows. Results are read from the final response without paging.

Invalid `Retry-After` headers, including delays outside the supported numeric range, fail the query instead of triggering an early retry.
