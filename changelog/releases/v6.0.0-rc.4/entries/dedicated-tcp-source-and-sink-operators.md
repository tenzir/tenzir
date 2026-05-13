---
title: Dedicated TCP source and sink operators
type: feature
author: mavam
prs:
  - 5744
  - 6017
created: 2026-04-30T12:58:34.082842Z
---

Tenzir now has dedicated TCP source and sink operators that match the same
client/server split as the HTTP operators:

- `from_tcp` connects to a remote TCP or TLS endpoint as a client.
- `accept_tcp` listens on a local endpoint and spawns a subpipeline per
  accepted connection. Inside the subpipeline, `$peer.ip` and `$peer.port`
  identify the connecting client; with `resolve_hostnames=true`,
  `$peer.hostname` is also available from reverse DNS.
- `to_tcp` connects to a remote endpoint and writes serialized bytes.
- `serve_tcp` listens for incoming connections and broadcasts pipeline output
  to all connected clients.

Each operator takes a parsing or printing subpipeline so connection
management, framing, and serialization stay separate concerns:

```tql
accept_tcp "0.0.0.0:8090" {
  read_json
}
```

```tql
to_tcp "collector.example.com:5044" {
  write_json
}
```

`from_tcp` and `to_tcp` reconnect with exponential backoff on connection
failure. All four operators support TLS via the `tls` option.

The legacy `load_tcp` and `save_tcp` operators are now deprecated. The
`tcp://` and `tcps://` URL schemes still dispatch to them via `from` and
`to`.
