---
title: "Support native plugins in the static binary"
type: bugfix
author: dominiklohmann
created: 2021-08-18T12:32:54Z
pr: 1850
---

The `segment-store` store backend and built-in transform steps (`hash`,
`replace`, and `delete`) now function correctly in static VAST binaries.
