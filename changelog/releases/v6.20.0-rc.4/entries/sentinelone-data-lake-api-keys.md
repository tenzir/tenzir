---
title: SentinelOne Data Lake API keys
type: feature
authors:
  - mavam
created: 2026-10-13T14:52:31.839973Z
---

`from_sentinelone_data_lake` now accepts `tenant=false` for SentinelOne Data Lake (SDL) API keys. Such a key scopes its queries to its own account, and previously every read without `account_ids` silently returned no events:

```tql
from_sentinelone_data_lake "https://<tenant>.sentinelone.net",
  token=secret("sentinelone-sdl-key"),
  tenant=false,
  start=now() - 1h
```

Without `account_ids`, the operator still queries every account that a console or service-user token can access by default. With `account_ids`, `tenant=true` is an error.
