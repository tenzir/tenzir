---
title: "Add optimizations for `if`"
type: change
author: dominiklohmann
created: 2025-04-16T20:36:17Z
pr: 5110
---

`1y` in TQL now equals 365.2425 days, which is the average length of a year in
the Gregorian calendar. This aligns the duration literal with the `years`
function and how the Tenzir Platform renders durations.
