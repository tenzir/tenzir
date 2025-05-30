---
title: "`save_email` cleanup"
type: bugfix
authors: raxyte
pr: 4848
---

The `endpoint` argument of the `save_email` operator was documented as optional
but was not parsed as so. This has been fixed and the argument is now
correctly optional.
