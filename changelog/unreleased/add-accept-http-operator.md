---
title: Add `accept_http` operator for receiving HTTP requests
type: change
authors:
  - lava
created: 2026-04-15T00:00:00.000000Z
---

We added a new operator to accept data from incoming HTTP connections.

The `server` option of the `from_http` operator is now deprecated.
Going forward, it should only be used for client-mode HTTP operations,
and the new `accept_http` operator should be used for server-mode
operations.
