---
title: "Don't overwrite index state after startup error"
type: bugfix
author: lava
created: 2020-08-24T12:03:21Z
pr: 1026
---

VAST would overwrite existing on-disk state data when encountering a partial
read during startup. This state-corrupting behavior no longer exists.
