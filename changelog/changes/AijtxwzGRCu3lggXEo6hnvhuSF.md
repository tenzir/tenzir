---
title: "Make data predicate evaluation column-major"
type: feature
authors: dominiklohmann
pr: 2730
---

Queries without acceleration from a dense index run significantly faster, e.g.,
initial tests show a 2x performance improvement for substring queries.
