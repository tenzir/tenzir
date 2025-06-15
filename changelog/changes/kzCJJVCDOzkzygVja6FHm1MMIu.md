---
title: "Unreliable `where` diagnostics"
type: bugfix
authors: jachris
pr: 5277
---

The `where` operator now correctly produces diagnostics also for simple
expressions, which was previously not the case in some situations.
