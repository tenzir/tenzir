---
title: "Add diagnostics (and some other improvements)"
type: feature
author: jachris
created: 2023-06-20T11:09:14Z
pr: 3223
---

In addition to `tenzir "<pipeline>"`, there now is `tenzir -f <file>`, which
loads and executes the pipeline defined in the given file.

The pipeline parser now emits helpful and visually pleasing diagnostics.
