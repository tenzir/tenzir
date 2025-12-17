---
title: "Dont abort JSON import when encountering non-objects"
type: bugfix
author: lava
created: 2021-07-06T09:54:32Z
pr: 1759
---

VAST does not abort JSON imports anymore when encountering
something other than a JSON object, e.g., a number or a string.
Instead, VAST skips the offending line.
