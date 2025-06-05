---
title: "Avoid shutdown when config dirs are not readable"
type: bugfix
authors: dominiklohmann
pr: 1533
---

VAST no longer refuses to start when any of the configuration file directories
is unreadable, e.g., because VAST is running in a sandbox.
