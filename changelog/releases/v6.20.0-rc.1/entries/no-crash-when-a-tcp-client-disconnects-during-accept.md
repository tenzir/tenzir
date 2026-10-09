---
title: No crash when a TCP client disconnects during accept
type: bugfix
authors:
  - tobim
created: 2026-10-07T13:35:37.132835Z
---

The node no longer crashes when a TCP client connects to a listening operator and resets the connection before the node has finished accepting it. Previously, this terminated the entire node with the message `setFromSocket() failed: Socket not connected`. Port scanners, load balancer health checks, and clients with short connect timeouts could trigger this. This affects all operators that accept incoming TCP connections, namely `accept_tcp`, `accept_relp`, and `serve_tcp`.
