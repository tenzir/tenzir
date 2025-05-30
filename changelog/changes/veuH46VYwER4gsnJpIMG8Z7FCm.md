---
title: "Use adaptive resolution and `Z` suffix in timestamp printer"
type: change
authors: jachris
pr: 4916
---

Timestamps are now printed with a `Z` suffix to indicate that they are relative
to UTC. Furthermore, the fractional part of the seconds is no longer always
printed using 6 digits. Instead, timestamps that do not have sub-second
information no longer have a fractional part. Other timestamps are either
printed with 3, 6 or 9 fractional digits, depending on their resolution.

Durations that are printed as minutes now use `min` instead of `m`.
Additionally, the fractional part of durations is now printed with full
precision instead of being rounded to two digits.
