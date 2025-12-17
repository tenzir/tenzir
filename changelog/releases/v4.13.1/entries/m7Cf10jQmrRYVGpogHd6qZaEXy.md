---
title: "Relax `transform_columns`"
type: bugfix
author: dominiklohmann
created: 2024-05-13T16:51:31Z
pr: 4215
---

The `enrich`, `drop`, `extend`, `replace`, and `deduplicate` operators failed
for empty input events. This no longer happens.
