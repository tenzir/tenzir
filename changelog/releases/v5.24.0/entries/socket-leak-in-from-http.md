---
title: Socket leak in `from_http`
type: bugfix
authors:
  - jachris
  - claude
pr: 5647
created: 2026-01-08T16:50:42.692996Z
---

The `from_http` operator sometimes left sockets in `CLOSE_WAIT` state instead of
closing them properly. This could lead to resource exhaustion on long-running
nodes receiving many HTTP requests.
