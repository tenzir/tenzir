---
title: "Fix `/pipeline/launch` when no cache is provided"
type: bugfix
authors: dominiklohmann
pr: 4554
---

We fixed a regression introduced with Tenzir v4.20 that sometimes caused the
Tenzir Platform to fail to fetch results from pipelines.
