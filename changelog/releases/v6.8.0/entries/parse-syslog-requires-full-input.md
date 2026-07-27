---
title: "`parse_syslog` rejects partially parsed input"
type: bugfix
authors:
  - jachris
  - claude
prs:
  - 6454
created: 2026-07-16T14:24:22.000000Z
---

`parse_syslog` now requires the parser to consume the entire input. Previously,
a message that parsed only partially was still accepted, and any trailing bytes
the parser did not reach were silently dropped. Such input is now rejected with
a diagnostic instead of being truncated.
