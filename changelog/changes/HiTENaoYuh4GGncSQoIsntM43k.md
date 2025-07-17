---
title: "Allow immediate restarts of the TCP listen connector"
type: bugfix
authors: tobim
pr: 4367
---

The `tcp` connector no longer fails in listen mode when you try to restart it
directly after stopping it.
