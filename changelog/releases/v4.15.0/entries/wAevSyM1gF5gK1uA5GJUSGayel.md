---
title: "Add label definitions to configured pipelines"
type: bugfix
author: tobim
created: 2024-05-30T16:40:25Z
pr: 4247
---

Pipelines configured as code no longer always restart with the node. Instead,
just like for other pipelines, they only restart when they were running before
the node shut down.
