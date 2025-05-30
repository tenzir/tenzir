---
title: "Fix deletion of removed configured contexts"
type: bugfix
authors: tobim
pr: 4330
---

We fixed a bug in Tenzir v4.17.2 that sometimes caused the deletion of on-disk
state of configured contexts on startup.
