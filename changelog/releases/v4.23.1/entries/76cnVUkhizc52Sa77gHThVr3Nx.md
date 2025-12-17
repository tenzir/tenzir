---
title: "Only wipe the cache directory contents but not the dir itself"
type: bugfix
author: lava
created: 2024-11-11T14:50:51Z
pr: 4742
---

The node doesn't try to recreate its cache directory on startup anymore,
avoiding permissions issues on systems with strict access control.
