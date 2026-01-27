---
title: "Use a proper subpipeline for `fork`"
type: bugfix
author: dominiklohmann
created: 2025-04-22T21:44:52Z
pr: 5133
---

Operators that interact with state in the node that is not local to the
pipeline, e.g., `context::update`, now properly work when used inside the nested
pipeline of the `fork` operator. Previously, pipelines of the form `fork {
context::update … }` failed at runtime.
