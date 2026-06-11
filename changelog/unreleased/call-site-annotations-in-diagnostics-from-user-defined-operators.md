---
title: Call-site annotations in diagnostics from user-defined operators
type: change
authors:
  - IyeOnline
prs:
  - 6350
created: 2026-06-11T14:31:08.280196Z
---

Diagnostics from inside a user-defined operator now include a "called from here"
annotation that points to the call site in the surrounding pipeline. This makes
it possible to immediately locate the offending call when a diagnostic is
emitted deep in a nested operator:

```
from {}
test::error
```

```
error: assertion failure
 --> <packages/test:error>:2:10
  |
2 |   assert false
  |          ^^^^^
  |
 --> <input>:2:1
  |
2 | test::error
  | ^^^^^^^^^^^ called from here
  |
```

Previously, such diagnostics contained no location information, making it
difficult to associate them with a specific call in the pipeline.
