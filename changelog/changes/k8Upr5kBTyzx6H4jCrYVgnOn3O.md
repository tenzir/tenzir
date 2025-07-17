---
title: "Generalize `every` to work with all operators"
type: feature
authors: dominiklohmann
pr: 4109
---

The `every <duration>` operator modifier now supports all operators, turning
blocking operators like `tail`, `sort` or `summarize` into operators that emit
events every `<duration>`.
