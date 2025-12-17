---
title: "Fix evaluation of `null if true else \u2026`"
type: bugfix
author: dominiklohmann
created: 2025-05-29T09:56:06Z
pr: 5150
---

The expression `null if true else 42` previously returned `42`. It now correctly
returns `null`.
