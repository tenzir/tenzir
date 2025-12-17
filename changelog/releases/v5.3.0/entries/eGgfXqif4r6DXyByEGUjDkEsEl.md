---
title: "`load_http` deprecated"
type: change
author: raxyte
created: 2025-06-02T19:19:46Z
pr: 5177
---

The `from` operator now dispatches to `from_http` for `http[s]` URLs.

The `load_http` operator is now deprecated in favor of `from_http`.
