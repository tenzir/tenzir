---
title: "Fix double-closing fds in the python operator"
type: bugfix
authors: lava
pr: 4646
---

Fixed a bug in the python operator that could lead to random valid file
descriptors in the parent process being closed prematurely.
