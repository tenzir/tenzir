---
title: Native OTLP ingestion
type: feature
authors:
  - mavam
created: 2026-08-13T12:28:20.851305Z
---

Receive OpenTelemetry logs, metrics, and traces directly with the new `accept_otlp` operator over OTLP/HTTP or OTLP/gRPC:

```tql
accept_otlp "0.0.0.0:4318", transport="http"
```

```tql
accept_otlp "0.0.0.0:4317", transport="grpc"
```

The receiver supports JSON and binary Protobuf over HTTP, unary gRPC, gzip compression, TLS and mTLS, signal filtering, bounded request sizes and concurrency, and lossless list-shaped attributes by default. Set `schema="record"` for direct access to literal attribute keys.
