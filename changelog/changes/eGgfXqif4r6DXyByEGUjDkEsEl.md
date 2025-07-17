---
title: "`load_http` deprecated"
type: change
authors: raxyte
pr: 5177
---

The `from` operator now dispatches to `from_http` for `http[s]` URLs.

The `load_http` operator is now deprecated in favor of `from_http`.
