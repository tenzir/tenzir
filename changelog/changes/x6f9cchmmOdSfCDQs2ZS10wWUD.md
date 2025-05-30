---
title: "Fix `sort` type check"
type: bugfix
authors: jachris
pr: 3655
---

Using the `sort` operator with polymorphic inputs no longer leads to a failing
assertion under some circumstances.
