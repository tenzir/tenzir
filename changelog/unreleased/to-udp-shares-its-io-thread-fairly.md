---
title: to_udp shares its I/O thread fairly
type: bugfix
authors:
  - jachris
created: 2026-11-27T14:03:05.409155Z
---

The `to_udp` operator now pauses every 256 datagrams to let other network I/O on the same thread proceed. Previously, a large batch of events delayed network traffic of other pipelines until all of its datagrams were sent.
