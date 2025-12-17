---
title: "Add a `rebuild` command plugin"
type: feature
author: dominiklohmann
created: 2022-06-09T15:44:23Z
pr: 2321
---

The new `rebuild` command rebuilds old partitions to take advantage
of improvements in newer VAST versions. Rebuilding takes place in the VAST
server in the background. This process merges partitions up to the configured
`max-partition-size`, turns VAST v1.x's heterogeneous into VAST v2.x's
homogenous partitions, migrates all data to the currently configured
`store-backend`, and upgrades to the most recent internal batch encoding and
indexes.
