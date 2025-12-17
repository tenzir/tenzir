---
title: "Fix shutdown of `every <interval> <transformation|sink>`"
type: bugfix
author: dominiklohmann
created: 2024-04-30T07:04:58Z
pr: 4166
---

Transformations or sinks used with the `every` operator modifier did not shut
down correctly when exhausting their input. This now work as expected.
