---
title: "Move event distribution statistics to the catalog"
type: change
author: dominiklohmann
created: 2023-01-12T14:29:26Z
pr: 2852
---

The per-schema event distribution moved from `index.statistics.layouts` to
`catalog.schemas`, and additionally includes information about the import time
range and the number of partitions VAST knows for the schema. The number of
events per schema no longer includes events that are yet unpersisted.
