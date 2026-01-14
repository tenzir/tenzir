---
title: "Compress serialized indexers"
type: feature
author: dominiklohmann
created: 2022-06-16T07:16:41Z
pr: 2346
---

VAST now compresses on-disk indexes with Zstd, resulting in a 50-80% size
reduction depending on the type of indexes used, and reducing the overall index
size to below the raw data size. This improves retention spans significantly.
For example, using the default configuration, the indexes for `suricata.ftp`
events now use 75% less disk space, and `suricata.flow` 30% less.
