---
title: "Render help and documentation on stdout"
type: change
authors: mavam
pr: 1385
---

The output of `vast help` and `vast documentation` now goes to *stdout* instead
of to stderr. Erroneous invocations of `vast` also print the helptext, but in
this case the output still goes to stderr to avoid interference with downstream
tooling.
