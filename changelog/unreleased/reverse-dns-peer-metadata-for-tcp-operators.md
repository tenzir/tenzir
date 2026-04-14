---
title: Reverse DNS peer metadata for TCP operators
type: feature
authors:
  - mavam
  - codex
created: 2026-04-14T18:19:29.233502Z
---

The `accept_tcp` operator now accepts `resolve_hostnames=true` to resolve
connected clients via reverse DNS and expose the result as `$peer.hostname`
inside the nested pipeline:

```tql
accept_tcp "0.0.0.0:514", resolve_hostnames=true {
  read_syslog
  collector = $peer
}
```

The `$peer.ip` field in both `accept_tcp` and `from_tcp` now uses the native
`ip` type, which makes it easier to use in TQL without converting it from a
string first.
