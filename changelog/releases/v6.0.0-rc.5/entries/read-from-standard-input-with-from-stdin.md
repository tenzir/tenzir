---
title: Read from standard input with `from_stdin`
type: feature
author: raxyte
pr: 5731
created: 2026-04-30T12:58:15.747791Z
---

The new `from_stdin` operator reads bytes from standard input through a
parsing subpipeline:

```tql
from_stdin {
  read_json
}
```

This is useful when piping data into the `tenzir` executable as part of a
shell script or command chain.
