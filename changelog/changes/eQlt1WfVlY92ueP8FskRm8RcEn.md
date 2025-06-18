---
title: "Allow fine-grained meta index configuration"
type: feature
authors: lava
pr: 2065
---

The new `vast.index` section in the configuration supports adjusting the
false-positive rate of first-stage lookups for individual fields, allowing
users to optimize the time/space trade-off for expensive queries.
