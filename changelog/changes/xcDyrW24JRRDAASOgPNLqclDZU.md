---
title: "Implement TQL2 `from` and `to`"
type: change
authors: IyeOnline
pr: 4805
---

The `topic` argument for `load_kafka` and `save_kafka` is now a positional
argument, instead of a named argument.

The array version of `from` that allowed you to create multiple events has been
removed. Instead, you can just pass multiple records to `from` now.
