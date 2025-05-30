---
title: "Add optimizations for `if`"
type: change
authors: dominiklohmann
pr: 5110
---

`1y` in TQL now equals 365.2425 days, which is the average length of a year in
the Gregorian calendar. This aligns the duration literal with the `years`
function and how the Tenzir Platform renders durations.
