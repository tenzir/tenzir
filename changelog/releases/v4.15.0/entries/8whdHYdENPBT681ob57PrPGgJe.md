---
title: "Support multiple table slice schemas when dumping contexts"
type: bugfix
author: Dakostu
created: 2024-05-21T15:24:55Z
pr: 4236
---

`context inspect` will not crash anymore when encountering contexts that
contain multi-schema data.
