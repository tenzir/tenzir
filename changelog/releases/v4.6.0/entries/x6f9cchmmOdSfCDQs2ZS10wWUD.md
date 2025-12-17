---
title: "Fix `sort` type check"
type: bugfix
author: jachris
created: 2023-11-20T16:44:49Z
pr: 3655
---

Using the `sort` operator with polymorphic inputs no longer leads to a failing
assertion under some circumstances.
