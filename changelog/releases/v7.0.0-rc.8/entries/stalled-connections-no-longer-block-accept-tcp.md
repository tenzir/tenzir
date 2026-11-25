---
title: Stalled connections no longer block other connections in accept_tcp
type: bugfix
authors:
  - tobim
created: 2026-11-13T09:18:25.423847Z
---

The `accept_tcp` and `accept_unix_socket` operators keep reading from all other connections when the subpipeline of one connection stops taking data. Previously, a single stalled connection made the operator stop reading from every connection and stop accepting new ones, so clients saw the listener hang even though most of the pipeline had capacity left.

Each connection now applies backpressure only to its own socket. When the entire pipeline after the operator falls behind, all connections still slow down together, as before.
