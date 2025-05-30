---
title: "Make partition deletion resilient against oversize"
type: bugfix
authors: tobim
pr: 2431
---

VAST is now able to detect corrupt index files and will attempt to repair them
on startup.
