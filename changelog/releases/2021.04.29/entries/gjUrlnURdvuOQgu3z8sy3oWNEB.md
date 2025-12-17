---
title: "Avoid shutdown when config dirs are not readable"
type: bugfix
author: dominiklohmann
created: 2021-04-08T08:27:20Z
pr: 1533
---

VAST no longer refuses to start when any of the configuration file directories
is unreadable, e.g., because VAST is running in a sandbox.
