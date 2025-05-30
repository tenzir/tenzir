---
title: "Fix URL check in `to_hive`"
type: bugfix
authors: jachris
pr: 5204
---

The `to_hive` operator no longer incorrectly rejects URLs, which was due to a
bug introduced by Tenzir v5.1.6.
