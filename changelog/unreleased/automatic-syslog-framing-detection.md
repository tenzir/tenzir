---
title: Automatic syslog framing detection
type: change
authors:
  - zedoraps
created: 2026-11-09T19:47:19.2706Z
---

The `read_syslog` operator now automatically detects octet-counted and
newline-delimited messages. A single TCP listener can receive both framing
methods from different clients:

```tql
accept_tcp "0.0.0.0:514" {
  read_syslog
}
```

Framing is selected independently for each connection. Existing pipelines that
set `octet_counting=true` or `octet_counting=false` keep their behavior and emit
a deprecation warning. Omit the parameter to enable automatic detection.

Automatic detection requires a positive decimal length, one ASCII space, and
a complete syslog priority header (`<PRI>`). Other input uses newline framing.
Use `octet_counting=true` for counted streams whose first message has no PRI
header.
