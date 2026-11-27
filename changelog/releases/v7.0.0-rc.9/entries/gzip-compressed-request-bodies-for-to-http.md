---
title: Gzip-compressed request bodies for to_http
type: feature
authors:
  - jachris
created: 2026-11-25T20:30:52.930594Z
---

The `to_http` operator has a new `compress` option that compresses the request body with gzip and sets the `Content-Encoding: gzip` header. This reduces the bytes sent over the network when the receiving server accepts gzip-encoded request bodies:

```tql
to_http "https://example.com/ingest", compress=true {
  write_ndjson
}
```

The option defaults to `false`, because many HTTP servers do not decompress request bodies. It works for both streamed and buffered bodies. Streamed bodies remain a single gzip stream whose data the server can decode as each chunk arrives. Combining `compress=true` with a manual `Content-Encoding` header is an error.
