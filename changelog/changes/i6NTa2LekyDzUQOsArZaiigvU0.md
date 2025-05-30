---
title: "Fix use-after-free bug in indexer state"
type: bugfix
authors: lava
pr: 896
---

A use after free bug would sometimes crash the node while it was shutting down.
