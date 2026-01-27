---
title: "Split `compress`/`decompress` into separate operators"
type: change
author: IyeOnline
created: 2025-01-06T09:50:54Z
pr: 4876
---

The `compress` and `decompress` operators have been deprecated in favor of
separate operators for each compression algorithm. These new operators expose
additional options, such as `compress_gzip level=10, format="deflate"`.
