---
title: "Fix `slice 1:-1` for exactly one event"
type: bugfix
authors: dominiklohmann
pr: 4505
---

The `slice` operator no longer crashes when used with a positive begin and
negative end value when operating on less events than `-end`, e.g., when working
on a single event and using `slice 0:-1`.
