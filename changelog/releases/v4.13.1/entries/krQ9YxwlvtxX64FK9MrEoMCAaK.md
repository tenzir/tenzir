---
title: "Improve `slice` with positive begin and negative end"
type: bugfix
author: dominiklohmann
created: 2024-05-13T08:56:29Z
pr: 4210
---

The `slice` operator no longer waits for all input to arrive when used with a
positive begin and a negative (or missing) end value, which rendered it unusable
with infinite inputs. Instead, the operator now yields results earlier.
