---
title: "Ignore spaces before SI prefixes"
type: bugfix
authors: dominiklohmann
pr: 1590
---

Spaces before SI prefixes in command line arguments and configuration options
are now generally ignored, e.g., it is now possible to set the disk monitor
budgets to `2 GiB` rather than `2GiB`.
