---
title: "Add `tail` operator"
type: feature
author: Dakostu
created: 2023-04-02T08:32:28Z
pr: 3050
---

The new `tail` pipeline operator limits all latest events to a specified
number. The operator takes the limit as an optional argument, with the default
value being 10.
