---
title: "Make `str(enum)` return the name of the enum entry"
type: bugfix
authors: jachris
pr: 4717
---

The `str` function no longer returns the numeric index of an enumeration value.
Instead, the result is now the actual name associated with that value.
