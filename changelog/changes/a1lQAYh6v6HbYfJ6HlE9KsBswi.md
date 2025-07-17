---
title: "Isolate environment in python operator"
type: bugfix
authors: jachris
pr: 4036
---

The code passed to the `python` operator no longer fails to resolve names when
the local and global scope are both used.
