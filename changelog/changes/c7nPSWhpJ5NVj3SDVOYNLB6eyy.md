---
title: "Include outdated but not undersized partitions in automatic rebuilds"
type: bugfix
authors: dominiklohmann
pr: 3020
---

Automatic partition rebuilding both updates partitions with an outdated storage
format and merges undersized partitions continuously in the background. This now
also works as expected for outdated but not undersized partitions.
