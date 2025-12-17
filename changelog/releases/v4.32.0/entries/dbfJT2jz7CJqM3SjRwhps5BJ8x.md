---
title: "Check array validity before iterating"
type: bugfix
author: tobim
created: 2025-04-04T09:48:43Z
pr: 5100
---

The `parse_json` function no longer crashes in case it encounters invalid
arrays.
