---
title: Stream pipeline output with `serve_http`
type: feature
author: aljazerzen
pr: 6070
created: 2026-04-30T12:59:33.02497Z
---

The new `serve_http` operator starts an HTTP server and broadcasts the bytes
produced by a nested pipeline to all connected clients:

```tql
from_file "example.yaml"
serve_http "0.0.0.0:8080" {
  write_ndjson
}
```

Clients connect with a `GET` request and receive a continuous HTTP response
body. Pick the wire format with the nested pipeline: `write_ndjson` for
NDJSON streams, `write_lines` for plain text, and so on. The operator does
not buffer output for clients that connect later—each client receives the
bytes produced after it connects. TLS, connection limits, and graceful
disconnect are all configurable.
