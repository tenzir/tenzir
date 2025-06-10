---
title: "Rework the plugin loading logic"
type: change
authors: dominiklohmann
pr: 1703
---

VAST no longer loads static plugins by default. Generally, VAST now
treats static plugins and bundled dynamic plugins equally, allowing
users to enable or disable static plugins as needed for their
deployments.
