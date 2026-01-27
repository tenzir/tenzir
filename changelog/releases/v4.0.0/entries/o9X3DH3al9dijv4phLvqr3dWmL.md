---
title: "Avoid crashing when reading a pre-2.0 partition"
type: feature
author: dominiklohmann
created: 2023-03-16T16:38:31Z
pr: 3018
---

The `flatten [<separator>]` operator flattens nested data structures by joining
nested records with the specified separator (defaults to `.`) and merging lists.
