---
title: Send logs to Grafana Loki with to_loki
type: feature
authors:
  - mavam
created: 2026-11-28T12:38:44Z
---

The new `to_loki` sink operator sends events to Grafana Loki's HTTP Push API (`POST /loki/api/v1/push`), supporting both Snappy-compressed protobuf (`encoding="protobuf"`, the default) and JSON (`encoding="json"`, gzip-compressed by default).

The operator defaults to `labels`, `line`, `timestamp`, and `structured_metadata` fields and also accepts custom expressions, including `line=this` to serialize structured events as compact JSON and `tenant=` to fan out multi-tenant streams via `X-Scope-OrgID`:

```tql
to_loki "https://loki.example.com",
  labels={app: service, env: environment},
  line=this,
  timestamp=ts,
  structured_metadata={trace_id: trace_id},
  tenant=org_id,
  headers={Authorization: "Bearer " + secret("loki-token")}
```

`to_loki` groups entries by tenant and canonical stream labels, sorts entries per stream by ascending timestamp, splits oversized batches cleanly while respecting `max_batch_size`, `max_batch_bytes`, and `flush_interval`, and automatically retries transient network errors, HTTP 429, and 5xx responses with exponential backoff and `Retry-After` handling.
