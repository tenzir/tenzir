---
title: Empty pipelines run as a no-op
type: bugfix
authors:
  - aljazerzen
  - claude
created: 2026-08-10T08:51:59.294041Z
---

Pipelines without any operators now run as a no-op instead of failing. Running
a pipeline whose definition is empty — for example because every line is
commented out — previously failed with `empty pipeline is not supported yet` on
the command line, and with an internal error when the pipeline ran on a node:

```text
error: pipeline failed

error: unexpected internal error: assertion `chain.size() != 0` failed
```

Such pipelines now complete successfully without producing any output, which
makes it safe to comment out the contents of an existing managed pipeline and
run it again.
