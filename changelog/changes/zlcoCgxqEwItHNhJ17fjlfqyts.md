---
title: "Introduce lambda functions"
type: bugfix
authors: dominiklohmann
pr: 5150
---

The expression `null if true else 42` previously returned `42`. It now correctly
returns `null`.
