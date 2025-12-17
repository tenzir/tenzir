---
title: "Support native systemd startup notification from VAST"
type: feature
author: lava
created: 2020-10-16T09:51:16Z
pr: 1091
---

When running VAST under systemd supervision, it is now possible to use the
`Type=notify` directive in the unit file to let VAST notify the service manager
when it becomes ready.
