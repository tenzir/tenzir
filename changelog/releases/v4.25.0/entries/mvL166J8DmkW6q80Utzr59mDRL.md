---
title: "`save_email` cleanup"
type: bugfix
author: raxyte
created: 2024-12-11T15:43:12Z
pr: 4848
---

The `endpoint` argument of the `save_email` operator was documented as optional
but was not parsed as so. This has been fixed and the argument is now
correctly optional.
