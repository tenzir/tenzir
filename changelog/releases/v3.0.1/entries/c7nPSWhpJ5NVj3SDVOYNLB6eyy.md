---
title: "Include outdated but not undersized partitions in automatic rebuilds"
type: bugfix
author: dominiklohmann
created: 2023-03-16T21:35:40Z
pr: 3020
---

Automatic partition rebuilding both updates partitions with an outdated storage
format and merges undersized partitions continuously in the background. This now
also works as expected for outdated but not undersized partitions.
