---
title: "Add optimizations for `if`"
type: bugfix
author: dominiklohmann
created: 2025-04-16T20:36:17Z
pr: 5110
---

Operators that interact with state in the node that is not local to the
pipeline, e.g., `context::update`, now properly work when used inside an `if`
statement. Previously, pipelines of the form `if … { context::update … }` failed
at runtime.

Branches in `if` statement no longer run on a single thread, and instead
properly participate in the thread pool. This fixes performance problems when
running complex pipelines inside branches. Note that this causes the output of
the `if` operator to be unordered between its branches.

Literal values of type `time` in expressions failed to parse when they used
subsecond prevision or a time-zone offset. This no longer happens.
