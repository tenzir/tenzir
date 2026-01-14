---
title: "Implement a fallback parser mechanism for extensions that don't have \u2026"
type: feature
author: Dakostu
created: 2023-08-04T08:01:29Z
pr: 3422
---

The `json` parser now servers as a fallback parser for all files whose
extension do not have any default parser in Tenzir.
