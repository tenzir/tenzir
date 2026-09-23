---
title: Crash in accept_relp during graceful shutdown
type: bugfix
authors:
  - mavam
created: 2026-09-23T11:33:24.677738Z
---

The `accept_relp` operator no longer occasionally crashes when a pipeline shuts down gracefully, for example when stopping a node or pipeline while RELP clients are connected. Previously, stopping the listener could terminate the process with a segmentation fault instead of draining the already accepted messages.
