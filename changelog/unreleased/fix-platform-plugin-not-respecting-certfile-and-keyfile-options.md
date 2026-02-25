---
title: Fix platform plugin not respecting `certfile` and `keyfile` options
type: bugfix
authors:
  - lava
created: 2026-02-24T16:40:29.768312Z
---

Fixed in issue where the platform plugin did not correctly use the
configured `certfile`, `keyfile` and `cafile` options for client
certificate authentication, and improved the error messages for TLS
issues during platform connection.
