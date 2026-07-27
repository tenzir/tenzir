---
title: Splunk search input operator
type: feature
authors:
  - zedoraps
  - codex
prs:
  - 6447
created: 2026-07-14T15:30:30.257667Z
---

The new `from_splunk` operator runs a bounded search against a Splunk Search
Head and emits every result as an event:

```tql
from_splunk "https://splunk.example.com:8089",
  search="search index=main sourcetype=linux_secure",
  earliest=now() - 15min,
  latest=now() - 5min,
  headers={Authorization: secret("splunk-authorization")}
```

The `earliest` and `latest` bounds take `time` values like the ones above or
strings in Splunk's native relative-time syntax, such as `earliest="-15m"` or
`earliest="-1h@h"` for snapping to the full hour.

The operator supports secret-valued authorization headers, TLS configuration,
request timeouts, and retries for recurring collection pipelines.
