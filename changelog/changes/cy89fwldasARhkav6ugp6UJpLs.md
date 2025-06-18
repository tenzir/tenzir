---
title: "Fix crash for heterogeneous subnet lookup tables"
type: bugfix
authors: dominiklohmann
pr: 4531
---

`context inspect <ctx>` no longer crashes for lookup table contexts with
values of multiple schemas when using subnets as keys.
