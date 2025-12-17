---
title: "Render help and documentation on stdout"
type: change
author: mavam
created: 2021-02-17T14:17:04Z
pr: 1385
---

The output of `vast help` and `vast documentation` now goes to *stdout* instead
of to stderr. Erroneous invocations of `vast` also print the helptext, but in
this case the output still goes to stderr to avoid interference with downstream
tooling.
