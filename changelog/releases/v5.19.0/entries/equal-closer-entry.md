---
title: "Lambda capture extraction"
type: bugfix
author: jachris
created: 2025-10-24T12:24:08Z
pr: 5538
---

Lambda captures now work correctly for field accesses where the left side is not
a constant field path. For example, `.map(x => a[x].b)` previously did not
capture `a`, even though that is required to correctly evaluate the body of the
lambda. This now works as expected.
