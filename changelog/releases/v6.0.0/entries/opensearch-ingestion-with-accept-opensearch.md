---
title: OpenSearch ingestion with `accept_opensearch`
type: breaking
author: aljazerzen
pr: 6066
created: 2026-04-30T12:59:18.006961Z
---

The new `accept_opensearch` operator starts an OpenSearch-compatible HTTP
server and turns incoming Bulk API requests into events:

```tql
accept_opensearch "0.0.0.0:9200"
publish "events"
```

The operator buffers each bulk request body up to `max_request_size`,
optionally decompresses it based on the `Content-Encoding` header, parses the
NDJSON payload, and emits the resulting records. Set `keep_actions=true` to
also keep the OpenSearch action objects (e.g., `{"create": ...}`) in the
stream.

The `from_opensearch` operator has been removed. Use `accept_opensearch`
instead. The `elasticsearch://` and `opensearch://` URL schemes now dispatch
to `accept_opensearch` via `from`.
