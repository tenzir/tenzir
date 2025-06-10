---
title: "Fix shutdown of the lookup helper actor"
type: bugfix
authors: tobim
pr: 4978
---

We sqashed a bug that prevented the `tenzir-node` process from exiting cleanly
while the `lookup` operator was used in a pipeline.
