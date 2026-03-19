---
title: Fix pattern equality ignoring case-insensitive flag
type: bugfix
authors:
  - jachris
pr: 5900
created: 2026-03-12T00:00:00Z
---

Pattern equality checks now correctly consider the case-insensitive flag.
Previously, two patterns that differed only in case sensitivity were treated as
equal, violating the hash/equality contract.
