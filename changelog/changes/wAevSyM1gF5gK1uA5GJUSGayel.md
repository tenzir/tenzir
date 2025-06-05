---
title: "Add label definitions to configured pipelines"
type: bugfix
authors: tobim
pr: 4247
---

Pipelines configured as code no longer always restart with the node. Instead,
just like for other pipelines, they only restart when they were running before
the node shut down.
