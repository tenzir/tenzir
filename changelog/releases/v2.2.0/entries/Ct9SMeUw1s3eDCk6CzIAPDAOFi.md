---
title: "Make partition deletion resilient against oversize"
type: bugfix
author: tobim
created: 2022-07-20T16:18:22Z
pr: 2431
---

VAST is now able to detect corrupt index files and will attempt to repair them
on startup.
