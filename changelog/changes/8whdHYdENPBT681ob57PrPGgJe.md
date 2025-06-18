---
title: "Support multiple table slice schemas when dumping contexts"
type: bugfix
authors: Dakostu
pr: 4236
---

`context inspect` will not crash anymore when encountering contexts that
contain multi-schema data.
