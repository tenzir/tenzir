---
title: "Add `export --internal` to access metrics"
type: bugfix
author: jachris
created: 2023-12-01T13:29:36Z
pr: 3619
---

`export --live` now respects a subsequent `where <expr>` instead of silently
discarding the filter expression.
