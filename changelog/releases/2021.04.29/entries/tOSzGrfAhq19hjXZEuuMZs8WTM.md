---
title: "Allow for building plugins separately from VAST"
type: bugfix
author: dominiklohmann
created: 2021-04-08T14:45:04Z
pr: 1532
---

Linking against an installed VAST via CMake now correctly resolves VAST's
dependencies.
