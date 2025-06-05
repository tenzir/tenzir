---
title: "Fix shutdown of `every <interval> <transformation|sink>`"
type: bugfix
authors: dominiklohmann
pr: 4166
---

Transformations or sinks used with the `every` operator modifier did not shut
down correctly when exhausting their input. This now work as expected.
