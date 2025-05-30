---
title: "Fix a race condition in `/serve`"
type: bugfix
authors: dominiklohmann
pr: 4123
---

We fixed a bug that caused the explorer to sometimes show 504 Gateway Timeout
errors for pipelines where the first result took over two seconds to arrive.
