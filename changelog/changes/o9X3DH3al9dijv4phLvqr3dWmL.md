---
title: "Avoid crashing when reading a pre-2.0 partition"
type: feature
authors: dominiklohmann
pr: 3018
---

The `flatten [<separator>]` operator flattens nested data structures by joining
nested records with the specified separator (defaults to `.`) and merging lists.
