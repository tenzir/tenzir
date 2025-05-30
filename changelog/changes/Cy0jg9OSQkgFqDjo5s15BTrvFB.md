---
title: "Add `export --internal` to access metrics"
type: bugfix
authors: jachris
pr: 3619
---

`export --live` now respects a subsequent `where <expr>` instead of silently
discarding the filter expression.
