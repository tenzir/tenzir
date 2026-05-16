---
title: Prometheus Remote Write sink
type: feature
authors:
  - mavam
  - codex
created: 2026-05-15T19:37:02.326776Z
---

The new `to_prometheus` operator sends metric events to Prometheus Remote Write receivers.

For example:

```tql
from {
  metric: "http_requests_total",
  value: 42,
  timestamp: 2026-05-15T10:00:00Z,
  labels: {method: "GET", status: 200},
}
to_prometheus "https://prometheus.example/api/v1/write"
```

The operator supports Prometheus Remote Write v1 by default and can send Remote Write v2 payloads with `protobuf_message="io.prometheus.write.v2.Request"`.
