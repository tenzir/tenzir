---
title: "Implement `move` operator"
type: feature
author: raxyte
created: 2025-04-17T09:27:07Z
pr: 5117
---

We added a new `move` operator that moves a field into another, effectively a
smart renaming such as `ctx.message=status.msg` moves the values from
`status.msg` into the field `message` of a record `ctx` and drops `status.msg`.
