---
title: "Remove events output from many context operators"
type: change
author: dominiklohmann
created: 2024-04-23T07:14:13Z
pr: 4143
---

The `context create`, `context reset`, `context update`, and `context load`
operators no return information about the context. Pipelines ending with these
operators will now be considered closed, and you will be asked to deploy them in
the Explorer. Previously, users commonly added `discard` after these operators
to force this behavior.
