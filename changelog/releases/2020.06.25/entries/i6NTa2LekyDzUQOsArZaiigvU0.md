---
title: "Fix use-after-free bug in indexer state"
type: bugfix
author: lava
created: 2020-06-09T17:19:57Z
pr: 896
---

A use after free bug would sometimes crash the node while it was shutting down.
