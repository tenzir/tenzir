---
title: "Remove the zero-size check in the split_at_null() input loop"
type: bugfix
authors: Dakostu
pr: 4341
---

We fixed a rarely occurring issue in the `gelf` parser that led to parsing
errors for some events.
