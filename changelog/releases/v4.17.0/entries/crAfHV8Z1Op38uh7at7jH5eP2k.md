---
title: "Make the parallel level in `lookup` configurable"
type: feature
author: dominiklohmann
created: 2024-06-18T16:21:11Z
pr: 4300
---

The `lookup` operator gained a new `--parallel <level>` option controlling the
number of partitions the operator is allowed to open at once for retrospective
lookups. This can significantly increase performance at the cost of higher
resource usage. The option defaults to 3. To restore the previous behavior, set
the option to 1.
