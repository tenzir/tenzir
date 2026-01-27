---
title: "Allow fine-grained meta index configuration"
type: feature
author: lava
created: 2022-03-21T18:26:46Z
pr: 2065
---

The new `vast.index` section in the configuration supports adjusting the
false-positive rate of first-stage lookups for individual fields, allowing
users to optimize the time/space trade-off for expensive queries.
