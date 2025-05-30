---
title: "Check array validity before iterating"
type: bugfix
authors: tobim
pr: 5100
---

The `parse_json` function no longer crashes in case it encounters invalid
arrays.
