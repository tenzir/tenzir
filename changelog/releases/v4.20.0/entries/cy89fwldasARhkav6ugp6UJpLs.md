---
title: "Fix crash for heterogeneous subnet lookup tables"
type: bugfix
author: dominiklohmann
created: 2024-08-26T16:23:45Z
pr: 4531
---

`context inspect <ctx>` no longer crashes for lookup table contexts with
values of multiple schemas when using subnets as keys.
