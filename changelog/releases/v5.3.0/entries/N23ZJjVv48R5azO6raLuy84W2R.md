---
title: "Implement `from_http` client"
type: feature
author: raxyte
created: 2025-06-02T19:19:46Z
pr: 5177
---

The `from_http` operator now supports HTTP client functionality. This allows
sending HTTP/1.1 requests, including support for custom methods, headers,
payloads, pagination, retries, and connection timeouts. The operator can be used
to fetch data from HTTP APIs and ingest it directly into pipelines.

Make a simple GET request auto-selecting the parser:

```tql
from_http "https://api.example.com/data"
```

Post data to some API:

```tql
from_http "https://api.example.com/submit", payload={foo: "bar"}.print_json(),
          headers={"Content-Type": "application/json"}
```

Paginating APIs:

```tql
from_http "https://api.example.com/items",
          paginate=(x => x.next_url if x.has_more? == true)
```
