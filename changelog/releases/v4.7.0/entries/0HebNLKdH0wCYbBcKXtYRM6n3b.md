---
title: "Add file data to show partitions"
type: feature
author: tobim
created: 2023-12-12T07:56:12Z
pr: 3675
---

`show partitions` now contains location and size of the `store`, `index`, and
`sketch` files of a partition, as well the aggregate size at `diskusage`.
