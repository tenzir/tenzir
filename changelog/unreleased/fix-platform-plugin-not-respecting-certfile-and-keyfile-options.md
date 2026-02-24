---
title: Fix platform plugin not respecting `certfile` and `keyfile` options
type: bugfix
authors:
  - lava
created: 2026-02-24T16:40:29.768312Z
---

The platform plugin did not correctly use the configured `certfile` and
`keyfile` options for client certificate authentication. Global TLS defaults
unconditionally overwrote plugin-specific settings, causing the custom CA and
client certificates to be replaced by the system trust store. The plugin now
correctly respects plugin-specific TLS configuration.
