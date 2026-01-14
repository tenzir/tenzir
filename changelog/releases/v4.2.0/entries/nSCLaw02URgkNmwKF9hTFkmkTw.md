---
title: "Add `lines` parser"
type: feature
author: mavam
created: 2023-09-13T10:33:28Z
pr: 3511
---

The new `lines` parser splits its input at newline characters and produces
events with a single field containing the line.
