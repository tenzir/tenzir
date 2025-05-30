---
title: "Introduce `{package,pipeline}::list`"
type: bugfix
authors: dominiklohmann
pr: 4746
---

`context inspect` crashed when used to inspect a context that was previously
updated with `context update` with an input containing a field of type `enum`.
This no longer happens.
