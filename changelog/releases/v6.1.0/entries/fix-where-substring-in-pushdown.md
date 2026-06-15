---
title: "Fix `where` substring filters dropping all events from `subscribe`"
type: bugfix
authors:
  - jachris
created: 2026-06-09T00:00:00.000000Z
---

A `where` filter that checks for a substring with the literal on the left, such
as `where "TRAFFIC" in log`, incorrectly removed every event when reading from
`subscribe`. Such filters now match correctly again.
