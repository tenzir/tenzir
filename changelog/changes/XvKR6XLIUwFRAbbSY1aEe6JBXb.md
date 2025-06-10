---
title: "Add support for reals in CSV without dot"
type: bugfix
authors: dominiklohmann
pr: 2184
---

The CSV parser no longer fails when encountering integers when floating point
values were expected.
