---
title: Warning when accept_tcp reaches its connection limit
type: change
authors:
  - tobim
created: 2026-11-18T12:25:12.397608Z
---

The `accept_tcp` and `accept_unix_socket` operators now emit a warning when all `max_connections` slots are in use. While the limit is reached, new clients wait until an open connection closes, which previously happened without any notice:

```text
warning: reached the connection limit of 128
  = note: endpoint: 0.0.0.0:514
  = note: new connections wait until an open connection closes
  = hint: increase `max_connections` to accept more concurrent clients
```

The warning repeats at most once every five minutes.
