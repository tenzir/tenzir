---
title: "Rework the plugin loading logic"
type: change
author: dominiklohmann
created: 2021-06-08T08:52:27Z
pr: 1703
---

VAST no longer loads static plugins by default. Generally, VAST now
treats static plugins and bundled dynamic plugins equally, allowing
users to enable or disable static plugins as needed for their
deployments.
