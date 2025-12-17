---
title: "Add support for reals in CSV without dot"
type: bugfix
author: dominiklohmann
created: 2022-04-05T15:23:15Z
pr: 2184
---

The CSV parser no longer fails when encountering integers when floating point
values were expected.
