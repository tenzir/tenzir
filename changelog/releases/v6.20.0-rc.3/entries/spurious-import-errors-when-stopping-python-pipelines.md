---
title: "Spurious import errors when stopping pipelines that use `python`"
type: bugfix
authors:
  - tobim
created: 2026-10-08T09:52:58.857333Z
---

Stopping a pipeline while its `python` operator is still starting no longer
logs misleading Python tracebacks such as `ModuleNotFoundError: No module named
'tenzir_operator'`. The operator now shuts down its Python process before it
removes the process's virtual environment.
