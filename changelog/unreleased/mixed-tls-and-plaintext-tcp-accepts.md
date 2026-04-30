---
title: Mixed TLS and plaintext TCP accepts
type: feature
authors:
  - mavam
  - codex
pr: 6107
created: 2026-04-30T14:48:06.683889Z
---

The `accept_tcp` operator can now accept plaintext and TLS clients on the same endpoint when you set `auto_detect_tls=true` together with a TLS configuration:

```tql
accept_tcp "0.0.0.0:514", tls={certfile: "server.pem", keyfile: "server.key"}, auto_detect_tls=true {
  read_lines
}
```

This helps during protocol migrations where some clients already use TLS while others still send plaintext to the same port.
