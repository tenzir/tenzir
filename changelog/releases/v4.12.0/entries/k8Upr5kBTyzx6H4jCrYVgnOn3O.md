---
title: "Generalize `every` to work with all operators"
type: feature
author: dominiklohmann
created: 2024-04-12T08:26:05Z
pr: 4109
---

The `every <duration>` operator modifier now supports all operators, turning
blocking operators like `tail`, `sort` or `summarize` into operators that emit
events every `<duration>`.
