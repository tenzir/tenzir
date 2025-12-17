---
title: "Remove the \"catalog\" and \"catalog-bytes\" keys from the index status"
type: change
author: tobim
created: 2022-04-27T10:00:03Z
pr: 2233
---

The `index` section in the status output no longer contains the `catalog` and
`catalog-bytes` keys. The information is already present in the top-level
`catalog` section.
