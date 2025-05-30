---
title: "Evaluate `ip == subnet` predicates"
type: bugfix
authors: dominiklohmann
pr: 4268
---

Predicates of the form `ip == subnet` and `ip in [subnet1, subnet2, …]` now
work as expected.

The `lookup` operator now correctly handles subnet keys when using the `--retro`
or `--snapshot` options.
