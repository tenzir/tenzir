---
title: "Make data predicate evaluation column-major"
type: feature
author: dominiklohmann
created: 2022-12-06T19:05:06Z
pr: 2730
---

Queries without acceleration from a dense index run significantly faster, e.g.,
initial tests show a 2x performance improvement for substring queries.
