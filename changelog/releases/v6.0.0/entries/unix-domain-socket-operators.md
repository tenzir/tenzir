---
title: Unix domain socket operators
type: feature
authors:
  - mavam
  - codex
created: 2026-05-30T00:00:00Z
---

Tenzir now supports Unix stream sockets in the new executor with three dedicated
operators:

- `from_unix_socket` connects to a Unix domain socket and parses incoming bytes.
- `accept_unix_socket` listens on a socket path and parses each accepted client stream.
- `to_unix_socket` connects to a socket path and writes serialized bytes.

These operators replace the previous `uds=true` options on `from_file` and
`to_file`. Use `from_unix_socket` or `to_unix_socket` for Unix domain socket clients, and keep
`from_file` and `to_file` for regular filesystem and object-storage access.

```tql
accept_unix_socket "/run/collector.sock" {
  read_json
}
```

```tql
to_unix_socket "/run/collector.sock" {
  write_ndjson
}
```
