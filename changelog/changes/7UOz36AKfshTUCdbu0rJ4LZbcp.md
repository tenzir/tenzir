---
title: "Handle loading of configured and non-configured contexts with the same name"
type: bugfix
authors: Dakostu
pr: 4224
---

Configured and non-configured contexts with the same name will not cause
non-deterministic behavior upon loading anymore. The node will shut down
instead.
