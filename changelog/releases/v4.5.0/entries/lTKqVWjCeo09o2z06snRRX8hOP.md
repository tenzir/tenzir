---
title: "Always enable time and bool synopses"
type: change
author: dominiklohmann
created: 2023-11-15T10:10:12Z
pr: 3639
---

Sparse indexes for time and bool fields are now always enabled, accelerating
lookups against them.
