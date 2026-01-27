---
title: "Improved Type Conflict Handling"
type: bugfix
authors:
  - IyeOnline
pr: 5612
created: 2025-12-16T13:50:15.093565Z
---

We resolved an issue that would appear when reading in lists (e.g. JSON `[]`)
where the elements had different types. Tenzir's type system at this time only
supports storing a single type in a list. Our parsers resolve this issue by first
attempting conversions (e.g. to a common numeric type) and turning all values
into strings as a last resort. Previously this would however also break Tenzir's
batch processing leading to significant performance loss. This has now been fixed.
