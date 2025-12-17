---
title: "Handle loading of configured and non-configured contexts with the same name"
type: bugfix
author: Dakostu
created: 2024-06-04T10:56:11Z
pr: 4224
---

Configured and non-configured contexts with the same name will not cause
non-deterministic behavior upon loading anymore. The node will shut down
instead.
