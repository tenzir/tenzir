---
title: NATS operators use a thread of their own
type: bugfix
authors:
  - jachris
created: 2026-11-27T14:12:09.91616Z
---

The `from_nats` and `to_nats` operators now handle their connections on a dedicated thread. Previously, a busy NATS connection kept a network thread that other pipelines share occupied, and adding or removing events of a connection from another thread was not properly synchronized.
