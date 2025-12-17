---
title: "Isolate environment in python operator"
type: bugfix
author: jachris
created: 2024-03-13T14:19:09Z
pr: 4036
---

The code passed to the `python` operator no longer fails to resolve names when
the local and global scope are both used.
