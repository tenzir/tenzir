---
title: "Implement a selection transform plugin"
type: feature
author: 6yozo
created: 2022-01-03T08:48:27Z
pr: 2014
---

VAST has a new transform step: `select`, which keeps rows matching the
configured expression and removes the rest from the input.
