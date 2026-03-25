---
title: Neo support for the python and shell operators
type: feature
authors:
  - tobim
  - codex
pr: 5948
created: 2026-03-25T13:12:46.757995Z
---

The neo execution engine now supports the `python` and `shell` operators, so pipelines that rely on subprocess-backed transformations work on the new async execution path.

For example, these pipelines now run with the neo executor:

```tql
from {x: 1}, {x: 2}
python "self.y = self.x * 2"
```

```tql
from {x: 1}, {x: 2}
write_lines
shell "wc -l"
read_lines
```

This makes it possible to keep using existing Python-based event transformations and shell-based byte-stream processing while moving to neo.
