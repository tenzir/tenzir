---
title: "Lambda capture extraction"
type: bugfix
authors: jachris
pr: 5538
---

Lambda captures now work correctly for field accesses where the left side is not
a constant field path. For example, `.map(x => a[x].b)` previously did not
capture `a`, even though that is required to correctly evaluate the body of the
lambda. This now works as expected.
