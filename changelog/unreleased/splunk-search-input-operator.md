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
  earliest="-15m",
  latest="-5m",
  headers={Authorization: secret("splunk-authorization")}
```

The operator supports secret-valued authorization headers, TLS configuration,
request timeouts, and retries for recurring collection pipelines.
