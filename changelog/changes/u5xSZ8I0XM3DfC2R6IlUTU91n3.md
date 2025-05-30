---
title: "Implement `move` operator"
type: feature
authors: raxyte
pr: 5117
---

We added a new `move` operator that moves a field into another, effectively a
smart renaming such as `ctx.message=status.msg` moves the values from
`status.msg` into the field `message` of a record `ctx` and drops `status.msg`.
