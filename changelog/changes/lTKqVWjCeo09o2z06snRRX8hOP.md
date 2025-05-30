---
title: "Always enable time and bool synopses"
type: change
authors: dominiklohmann
pr: 3639
---

Sparse indexes for time and bool fields are now always enabled, accelerating
lookups against them.
