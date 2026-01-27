---
title: "Remove the zero-size check in the split_at_null() input loop"
type: bugfix
author: Dakostu
created: 2024-07-02T13:35:11Z
pr: 4341
---

We fixed a rarely occurring issue in the `gelf` parser that led to parsing
errors for some events.
