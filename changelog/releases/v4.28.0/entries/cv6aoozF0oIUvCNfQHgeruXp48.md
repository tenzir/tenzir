---
title: "Fix shutdown of the lookup helper actor"
type: bugfix
author: tobim
created: 2025-02-06T21:15:14Z
pr: 4978
---

We sqashed a bug that prevented the `tenzir-node` process from exiting cleanly
while the `lookup` operator was used in a pipeline.
