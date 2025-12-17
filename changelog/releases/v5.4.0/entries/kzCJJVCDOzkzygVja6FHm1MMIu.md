---
title: "Unreliable `where` diagnostics"
type: bugfix
author: jachris
created: 2025-06-12T13:28:08Z
pr: 5277
---

The `where` operator now correctly produces diagnostics also for simple
expressions, which was previously not the case in some situations.
