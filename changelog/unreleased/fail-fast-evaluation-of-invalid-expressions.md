---
title: Fail-fast evaluation of invalid expressions
type: bugfix
authors:
  - aljazerzen
created: 2026-09-30T09:38:37.130958Z
---

Unexpected expression forms now fail immediately instead of silently yielding null values during pipeline evaluation. This makes internal evaluation errors visible rather than producing incorrect results.
