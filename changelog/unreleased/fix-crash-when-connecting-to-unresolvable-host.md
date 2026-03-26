---
title: Fix crash when connecting to unresolvable host
type: bugfix
author: lava
pr: 5827
created: 2026-03-26T11:47:48.810386Z
---

Connecting a connector to a hostname that cannot be resolved no longer crashes the pipeline with a segfault.
