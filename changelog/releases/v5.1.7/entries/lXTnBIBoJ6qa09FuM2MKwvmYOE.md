---
title: "Fix URL check in `to_hive`"
type: bugfix
author: jachris
created: 2025-05-16T17:33:41Z
pr: 5204
---

The `to_hive` operator no longer incorrectly rejects URLs, which was due to a
bug introduced by Tenzir v5.1.6.
