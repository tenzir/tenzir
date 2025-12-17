---
title: "Ignore spaces before SI prefixes"
type: bugfix
author: dominiklohmann
created: 2021-04-27T08:08:04Z
pr: 1590
---

Spaces before SI prefixes in command line arguments and configuration options
are now generally ignored, e.g., it is now possible to set the disk monitor
budgets to `2 GiB` rather than `2GiB`.
