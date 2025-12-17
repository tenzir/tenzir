---
title: "Fix JSON printer handling of `inf` and `nan`"
type: bugfix
author: jachris
created: 2024-04-03T14:55:03Z
pr: 4087
---

The JSON printer previously printed invalid JSON for `inf` and `nan`, which
means that `serve` could sometimes emit invalid JSON, which is not handled well
by platform/app. Instead, we now emit `null`.
