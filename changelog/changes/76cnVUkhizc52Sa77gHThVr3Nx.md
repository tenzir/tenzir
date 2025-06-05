---
title: "Only wipe the cache directory contents but not the dir itself"
type: bugfix
authors: lava
pr: 4742
---

The node doesn't try to recreate its cache directory on startup anymore,
avoiding permissions issues on systems with strict access control.
