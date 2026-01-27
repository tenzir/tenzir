---
title: "Allow immediate restarts of the TCP listen connector"
type: bugfix
author: tobim
created: 2024-07-09T18:12:49Z
pr: 4367
---

The `tcp` connector no longer fails in listen mode when you try to restart it
directly after stopping it.
