---
title: "Relax `transform_columns`"
type: bugfix
authors: dominiklohmann
pr: 4215
---

The `enrich`, `drop`, `extend`, `replace`, and `deduplicate` operators failed
for empty input events. This no longer happens.
