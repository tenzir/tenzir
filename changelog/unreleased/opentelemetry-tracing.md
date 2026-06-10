---
title: OpenTelemetry tracing
type: feature
authors:
  - jachris
  - claude
prs:
  - 6273
created: 2026-06-10T11:51:20.695295Z
---

Tenzir nodes can now emit OpenTelemetry traces over OTLP/HTTP. Point the node at
an OpenTelemetry collector to enable it:

```yaml
tenzir:
  opentelemetry:
    endpoint: http://localhost:4318/v1/traces
```

Tracing is disabled until an endpoint is configured. Once enabled, the node
traces control-plane pipeline operations—creating, updating, deleting, and
launching pipelines—and propagates W3C trace context so these spans link to the
upstream request that triggered them.
