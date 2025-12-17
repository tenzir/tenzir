---
title: "Shutdown node when component startup fails"
type: bugfix
author: mavam
created: 2020-08-24T15:32:57Z
pr: 1028
---

VAST did not terminate when a critical component failed during startup. VAST
now binds the lifetime of the node to all critical components.
