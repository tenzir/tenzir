---
title: Bounded memory in accept_tcp with short-lived connections
type: bugfix
authors:
  - aljazerzen
created: 2026-11-10T12:41:48.171195Z
---

The `accept_tcp` and `accept_unix_socket` operators no longer use unbounded memory when clients open a new connection per message and the rest of the pipeline cannot keep up. Previously, the operators freed a `max_connections` slot as soon as a client closed its connection, even though the subpipeline for that connection was still waiting to pass on its events. These waiting subpipelines could pile up without limit, which caused out-of-memory kills.

Now, a connection holds its slot until its subpipeline finishes. When the pipeline falls behind, the operators stop accepting new connections, and clients wait in the listen backlog until a slot becomes free.
