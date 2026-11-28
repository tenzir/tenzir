---
title: Coalescing else keeps null fallback under filter optimization
type: bugfix
authors:
  - mavam
created: 2026-11-27T23:32:32.715774Z
---

Fixed a filter-optimization bug where rewriting or simplifying a null-coalescing `else` expression over an `if` assignment could turn it into a ternary conditional and drop matching events:

```tql
from {id: 1, raw: 1, b: null, c: true}
set x = raw?, y = null else (b? if c)
where (y else x) == 1
```

Previously, the optimizer simplified `y` to `b? if c` inside `y else x`, producing `(b? if c) else raw?` and returning `null` instead of falling back to `raw?`. Filter substitution and simplification now preserve null-coalescing semantics.
