---
title: "Support native systemd startup notification from VAST"
type: feature
authors: lava
pr: 1091
---

When running VAST under systemd supervision, it is now possible to use the
`Type=notify` directive in the unit file to let VAST notify the service manager
when it becomes ready.
