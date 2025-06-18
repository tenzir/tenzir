---
title: "Change Arrow extension type and metadata prefixes"
type: change
authors: dominiklohmann
pr: 3208
---

We now register extension types as `tenzir.ip`, `tenzir.subnet`, and
`tenzir.enumeration` instead of `vast.address`, `vast.subnet`, and
`vast.enumeration`, respectively. Arrow schema metadata now has a `TENZIR:`
prefix instead of a `VAST:` prefix.
