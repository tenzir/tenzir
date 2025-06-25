---
title: "`from_file` with a per-file sink"
type: bugfix
authors: dominiklohmann
pr: 5303
---

The `from_file` operator no longer fails when its per-file pipeline argument is
a sink. Before this fix, the following pipeline which opens a new TCP connection
per file would not work:

```tql
from_file "./*.csv" {
  read_csv
  save_tcp "localhost:8080"
}
```
