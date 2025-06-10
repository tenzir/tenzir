---
title: "Shutdown node when component startup fails"
type: bugfix
authors: mavam
pr: 1028
---

VAST did not terminate when a critical component failed during startup. VAST
now binds the lifetime of the node to all critical components.
