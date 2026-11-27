---
title: accept_tcp closes connections to vanished peers
type: bugfix
authors:
  - tobim
created: 2026-11-18T12:25:12.397608Z
---

The `accept_tcp` operator now detects clients that disappeared without closing their connection, for example because a NAT gateway or firewall dropped the connection state or the client host lost power. Previously, such connections stayed open indefinitely and kept occupying one of the `max_connections` slots, so the operator could eventually stop accepting new clients.

The operator now enables TCP keepalive on accepted connections and closes a connection when its peer does not respond for about five minutes. Clients that are connected but idle are not affected.
