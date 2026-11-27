---
title: Lower CPU usage of from_nic on busy interfaces
type: bugfix
authors:
  - jachris
created: 2026-11-26T17:22:15.915506Z
---

The `from_nic` operator no longer copies all packets it has buffered so far whenever it captures another packet. On busy interfaces, this made packet capture far more CPU-intensive than necessary and could delay other network I/O in the same process.
