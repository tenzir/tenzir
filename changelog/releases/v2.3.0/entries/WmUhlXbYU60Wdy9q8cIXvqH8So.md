---
title: "Wait until all stores have exited before finishing a partition transform"
type: bugfix
author: lava
created: 2022-08-31T09:24:42Z
pr: 2543
---

Fixed a race condition where the output of a partition transform
could be reused before it was fully written to disk, for example
when running `vast rebuild`.
