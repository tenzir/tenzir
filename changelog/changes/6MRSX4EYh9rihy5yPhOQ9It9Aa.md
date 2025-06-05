---
title: "Fix `from`/`to` not respecting default formats"
type: feature
authors: IyeOnline
pr: 4990
---

The `from` and `to` operators now assume `http` and `https` URLs to produce or
accept JSON, unless the filename in the URL contains a known file extension.
