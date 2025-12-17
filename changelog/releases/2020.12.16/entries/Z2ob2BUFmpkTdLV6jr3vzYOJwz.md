---
title: "Remove Version FlatBuffers table"
type: change
author: dominiklohmann
created: 2020-11-16T07:51:17Z
pr: 1168
---

Archive segments no longer include an additional, unnecessary version
identifier. We took the opportunity to clean this up bundled with the other
recent breaking changes.
