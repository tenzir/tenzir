---
title: "Fix a bunch of smaller issues"
type: bugfix
author: dominiklohmann
created: 2021-05-31T10:06:39Z
pr: 1697
---

Building plugins against an installed VAST no longer requires manually
specifying `-DBUILD_SHARED_LIBS=ON`. The option is now correctly enabled by
default for external plugins.
