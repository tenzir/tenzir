---
title: '`from_http` is now a pure HTTP client'
type: breaking
author: aljazerzen
pr: 5953
created: 2026-04-30T13:00:30.348949Z
---

The `from_http` operator no longer doubles as an HTTP server. It is now a
pure HTTP client that issues one request and returns the response.

For accepting incoming HTTP requests, use the dedicated `accept_http`
operator instead:

```tql
// Before:
from_http "0.0.0.0:8080", server=true { read_json }

// After:
accept_http "0.0.0.0:8080" { read_json }
```

In the parsing subpipeline, the response metadata is now exposed as the
`$response` let-binding instead of being written into a `metadata_field`:

```tql
from_http "https://api.example.com/status" {
  read_json
  status_code = $response.code
  server = $response.headers.Server
}
```

Additionally, the `url` and `headers` arguments are now resolved as
[secrets](https://docs.tenzir.com/explanations/secrets), so you can pass
secret names instead of hardcoding tokens or sensitive URLs.
